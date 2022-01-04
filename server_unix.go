// Copyright (c) 2019 Andy Pan
// Copyright (c) 2018 Joshua J Baker
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package gnet

import (
	"context"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/internal/netpoll"
	"github.com/panjf2000/gnet/pkg/errors"
)

type server struct {
	lns          []*listener        // the listener for accepting new connections
	lb           loadBalancer       // event-loops for handling events
	wg           sync.WaitGroup     // event-loop close WaitGroup
	opts         *Options           // options with server
	once         sync.Once          // make sure only signalShutdown once
	cond         *sync.Cond         // shutdown signaler
	mainLoop     *eventloop         // main event-loop for accepting connections
	inShutdown   int32              // whether the server is in shutdown
	tickerCtx    context.Context    // context for ticker
	cancelTicker context.CancelFunc // function to stop the ticker
	eventHandler EventHandler       // user eventHandler
}

func (svr *server) isInShutdown() bool {
	return atomic.LoadInt32(&svr.inShutdown) == 1
}

// waitForShutdown waits for a signal to shut down.
func (svr *server) waitForShutdown() {
	svr.cond.L.Lock()
	svr.cond.Wait()
	svr.cond.L.Unlock()
}

// signalShutdown signals the server to shut down.
func (svr *server) signalShutdown() {
	svr.once.Do(func() {
		svr.cond.L.Lock()
		svr.cond.Signal()
		svr.cond.L.Unlock()
	})
}

func (svr *server) startEventLoops() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			el.run(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) closeEventLoops() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

func (svr *server) startSubReactors() {
	svr.lb.iterate(func(i int, el *eventloop) bool {
		svr.wg.Add(1)
		go func() {
			el.activateSubReactor(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
		return true
	})
}

func (svr *server) activateEventLoops(numEventLoop int) (err error) {
	var striker *eventloop
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		lns := make([]*listener, len(svr.lns))
		for j, ln := range svr.lns {
			//network, address := svr.ln.network, svr.ln.address
			//ln := svr.ln
			//svr.ln = nil
			lns[j] = ln
			if i > 0 && (svr.opts.ReusePort || ln.network == "udp") {
				if ln, err = initListener(svr.lns[j].network, svr.lns[j].address, svr.opts); err != nil {
					return
				}
				lns[j] = ln
			}
		}
		var p *netpoll.Poller
		if p, err = netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.lns = lns
			el.svr = svr
			el.poller = p
			el.buffer = make([]byte, svr.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = svr.eventHandler
			for _, ln := range el.lns {
				if err = el.poller.AddRead(ln.packPollAttachment(el.accept)); err != nil {
					continue
				}
			}
			svr.lb.register(el)

			// Start the ticker.
			if el.idx == 0 && svr.opts.Ticker {
				striker = el
			}
		} else {
			return
		}
	}

	// Start event-loops in background.
	svr.startEventLoops()

	go striker.ticker(svr.tickerCtx)

	return
}

func (svr *server) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.lns = svr.lns
			el.svr = svr
			el.poller = p
			el.buffer = make([]byte, svr.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = svr.eventHandler
			svr.lb.register(el)
		} else {
			return err
		}
	}

	// Start sub reactors in background.
	svr.startSubReactors()

	if p, err := netpoll.OpenPoller(); err == nil {
		el := new(eventloop)
		el.lns = svr.lns
		el.idx = -1
		el.svr = svr
		el.poller = p
		el.eventHandler = svr.eventHandler
		for _, ln := range svr.lns {
			_ = el.poller.AddRead(ln.packPollAttachment(svr.accept))
		}
		svr.mainLoop = el

		// Start main reactor in background.
		svr.wg.Add(1)
		go func() {
			el.activateMainReactor(svr.opts.LockOSThread)
			svr.wg.Done()
		}()
	} else {
		return err
	}

	// Start the ticker.
	if svr.opts.Ticker {
		go svr.mainLoop.ticker(svr.tickerCtx)
	}

	return nil
}

func (svr *server) start(numEventLoop int) error {
	// TODO:
	if svr.opts.ReusePort || hasUdp(svr.lns) {
		return svr.activateEventLoops(numEventLoop)
	}

	return svr.activateReactors(numEventLoop)
}

func (svr *server) stop(s Server) {
	// Wait on a signal for shutdown
	svr.waitForShutdown()

	svr.eventHandler.OnShutdown(s)

	// Notify all loops to close by closing all listeners
	svr.lb.iterate(func(i int, el *eventloop) bool {
		err := el.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrServerShutdown }, nil)
		if err != nil {
			svr.opts.Logger.Errorf("failed to call UrgentTrigger on sub event-loop when stopping server: %v", err)
		}
		return true
	})

	if svr.mainLoop != nil {
		for _, ln := range svr.lns {
			ln.close()
		}
		err := svr.mainLoop.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrServerShutdown }, nil)
		if err != nil {
			svr.opts.Logger.Errorf("failed to call UrgentTrigger on main event-loop when stopping server: %v", err)
		}
	}

	// Wait on all loops to complete reading events
	svr.wg.Wait()

	svr.closeEventLoops()

	if svr.mainLoop != nil {
		err := svr.mainLoop.poller.Close()
		if err != nil {
			svr.opts.Logger.Errorf("failed to close poller when stopping server: %v", err)
		}
	}

	// Stop the ticker.
	if svr.opts.Ticker {
		svr.cancelTicker()
	}

	atomic.StoreInt32(&svr.inShutdown, 1)
}

func serve(eventHandler EventHandler, listeners []*listener, options *Options, protoAddrs []string) error {
	// Figure out the proper number of event-loops/goroutines to run.
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 0 {
		numEventLoop = options.NumEventLoop
	}

	svr := new(server)
	svr.opts = options
	svr.eventHandler = eventHandler
	svr.lns = listeners

	switch options.LB {
	case RoundRobin:
		svr.lb = new(roundRobinLoadBalancer)
	case LeastConnections:
		svr.lb = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		svr.lb = new(sourceAddrHashLoadBalancer)
	}

	svr.cond = sync.NewCond(&sync.Mutex{})
	if svr.opts.Ticker {
		svr.tickerCtx, svr.cancelTicker = context.WithCancel(context.Background())
	}
	if options.Codec == nil {
		svr.opts.Codec = new(BuiltInFrameCodec)
	}

	s := Server{
		svr:          svr,
		Multicore:    options.Multicore,
		Addrs:        make([]net.Addr, len(listeners)),
		NumEventLoop: numEventLoop,
		ReusePort:    options.ReusePort,
		TCPKeepAlive: options.TCPKeepAlive,
	}

	for i, ln := range listeners {
		s.Addrs[i] = ln.addr
	}

	switch svr.eventHandler.OnInitComplete(s) {
	case None:
	case Shutdown:
		return nil
	}

	if err := svr.start(numEventLoop); err != nil {
		svr.closeEventLoops()
		svr.opts.Logger.Errorf("gnet server is stopping with error: %v", err)
		return err
	}
	defer svr.stop(s)

	allServers.Store(strings.Join(protoAddrs, ";"), svr)

	return nil
}

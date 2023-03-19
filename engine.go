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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/panjf2000/gnet/v2/internal/netpoll"
	"github.com/panjf2000/gnet/v2/pkg/errors"
)

type engine struct {
	listeners    map[int]*listener  // listeners for accepting new connections
	lb           loadBalancer       // event-loops for handling events
	wg           sync.WaitGroup     // event-loop close WaitGroup
	opts         *Options           // options with engine
	once         sync.Once          // make sure only signalShutdown once
	cond         *sync.Cond         // shutdown signaler
	mainLoop     *eventloop         // main event-loop for accepting connections
	inShutdown   int32              // whether the engine is in shutdown
	tickerCtx    context.Context    // context for ticker
	cancelTicker context.CancelFunc // function to stop the ticker
	eventHandler EventHandler       // user eventHandler
}

func (eng *engine) isInShutdown() bool {
	return atomic.LoadInt32(&eng.inShutdown) == 1
}

// waitForShutdown waits for a signal to shut down.
func (eng *engine) waitForShutdown() {
	eng.cond.L.Lock()
	eng.cond.Wait()
	eng.cond.L.Unlock()
}

// signalShutdown signals the engine to shut down.
func (eng *engine) signalShutdown() {
	eng.once.Do(func() {
		eng.cond.L.Lock()
		eng.cond.Signal()
		eng.cond.L.Unlock()
	})
}

func (eng *engine) startEventLoops() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		eng.wg.Add(1)
		go func() {
			el.run(eng.opts.LockOSThread)
			eng.wg.Done()
		}()
		return true
	})
}

func (eng *engine) closeEventLoops() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		_ = el.poller.Close()
		return true
	})
}

func (eng *engine) startSubReactors() {
	eng.lb.iterate(func(i int, el *eventloop) bool {
		eng.wg.Add(1)
		go func() {
			el.activateSubReactor(eng.opts.LockOSThread)
			eng.wg.Done()
		}()
		return true
	})
}

func (eng *engine) activateEventLoops(numEventLoop int) (err error) {
	listeners := eng.listeners
	eng.listeners = nil
	var striker *eventloop
	// Create loops locally and bind the listeners.
	for i := 0; i < numEventLoop; i++ {
		if i > 0 {
			ls := make(map[int]*listener, len(listeners))
			for _, ln := range listeners {
				if ln, err = initListener(ln.network, ln.address, eng.opts); err != nil {
					return
				}
				ls[ln.fd] = ln
			}
			listeners = ls
		}
		var p *netpoll.Poller
		if p, err = netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.listeners = listeners
			el.engine = eng
			el.poller = p
			el.buffer = make([]byte, eng.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = eng.eventHandler
			for _, ln := range listeners {
				if err = el.poller.AddRead(ln.packPollAttachment(el.accept)); err != nil {
					return
				}
			}
			eng.lb.register(el)

			// Start the ticker.
			if el.idx == 0 && eng.opts.Ticker {
				striker = el
			}
		} else {
			return
		}
	}

	// Start event-loops in background.
	eng.startEventLoops()

	go striker.ticker(eng.tickerCtx)

	return
}

func (eng *engine) activateReactors(numEventLoop int) error {
	for i := 0; i < numEventLoop; i++ {
		if p, err := netpoll.OpenPoller(); err == nil {
			el := new(eventloop)
			el.listeners = eng.listeners
			el.engine = eng
			el.poller = p
			el.buffer = make([]byte, eng.opts.ReadBufferCap)
			el.connections = make(map[int]*conn)
			el.eventHandler = eng.eventHandler
			eng.lb.register(el)
		} else {
			return err
		}
	}

	// Start sub reactors in background.
	eng.startSubReactors()

	if p, err := netpoll.OpenPoller(); err == nil {
		el := new(eventloop)
		el.listeners = eng.listeners
		el.idx = -1
		el.engine = eng
		el.poller = p
		el.eventHandler = eng.eventHandler
		for _, ln := range eng.listeners {
			if err = el.poller.AddRead(ln.packPollAttachment(eng.accept)); err != nil {
				return err
			}
		}
		eng.mainLoop = el

		// Start main reactor in background.
		eng.wg.Add(1)
		go func() {
			el.activateMainReactor(eng.opts.LockOSThread)
			eng.wg.Done()
		}()
	} else {
		return err
	}

	// Start the ticker.
	if eng.opts.Ticker {
		go eng.mainLoop.ticker(eng.tickerCtx)
	}

	return nil
}

func (eng *engine) start(numEventLoop int) error {
	network := ""
	for _, ln := range eng.listeners {
		network = ln.network
		break
	}
	if eng.opts.ReusePort || network == "udp" {
		return eng.activateEventLoops(numEventLoop)
	}

	return eng.activateReactors(numEventLoop)
}

func (eng *engine) stop(s Engine) {
	// Wait on a signal for shutdown
	eng.waitForShutdown()

	eng.eventHandler.OnShutdown(s)

	// Notify all loops to close by closing all listeners
	eng.lb.iterate(func(i int, el *eventloop) bool {
		err := el.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Errorf("failed to call UrgentTrigger on sub event-loop when stopping engine: %v", err)
		}
		return true
	})

	if eng.mainLoop != nil {
		eng.mainLoop.closeAllListeners()
		err := eng.mainLoop.poller.UrgentTrigger(func(_ interface{}) error { return errors.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Errorf("failed to call UrgentTrigger on main event-loop when stopping engine: %v", err)
		}
	}

	// Wait on all loops to complete reading events
	eng.wg.Wait()

	eng.closeEventLoops()

	if eng.mainLoop != nil {
		err := eng.mainLoop.poller.Close()
		if err != nil {
			eng.opts.Logger.Errorf("failed to close poller when stopping engine: %v", err)
		}
	}

	// Stop the ticker.
	if eng.opts.Ticker {
		eng.cancelTicker()
	}

	atomic.StoreInt32(&eng.inShutdown, 1)
}

func run(eventHandler EventHandler, listeners map[int]*listener, options *Options, protoAddr string) error {
	// Figure out the proper number of event-loops/goroutines to run.
	numEventLoop := 1
	if options.Multicore {
		numEventLoop = runtime.NumCPU()
	}
	if options.NumEventLoop > 0 {
		numEventLoop = options.NumEventLoop
	}

	eng := new(engine)
	eng.opts = options
	eng.eventHandler = eventHandler
	eng.listeners = listeners

	switch options.LB {
	case RoundRobin:
		eng.lb = new(roundRobinLoadBalancer)
	case LeastConnections:
		eng.lb = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		eng.lb = new(sourceAddrHashLoadBalancer)
	}

	eng.cond = sync.NewCond(&sync.Mutex{})
	if eng.opts.Ticker {
		eng.tickerCtx, eng.cancelTicker = context.WithCancel(context.Background())
	}

	e := Engine{eng}
	switch eng.eventHandler.OnBoot(e) {
	case None:
	case Shutdown:
		return nil
	}

	if err := eng.start(numEventLoop); err != nil {
		eng.closeEventLoops()
		eng.opts.Logger.Errorf("gnet engine is stopping with error: %v", err)
		return err
	}
	defer eng.stop(e)

	allEngines.Store(protoAddr, eng)

	return nil
}

// AsyncWrite - AsyncWrite
func (eng *engine) AsyncWrite(connId int64, data []byte) {
	elidx := int(connId >> 48 & 0xffff)
	id := uint16(connId >> 32 & 0xffff)
	fd := int(connId & 0xffffffff)

	eng.lb.iterate(func(i int, el *eventloop) bool {
		if i == elidx {
			_ = el.poller.Trigger(func(_ interface{}) error {
				if c, ok := el.connections[fd]; ok && c.id == id {
					if !c.opened {
						return nil
					}
					c.write(data)
				}
				return nil
			}, nil)
			return false
		}
		return true
	})
}

// Trigger - Trigger
func (eng *engine) Trigger(connId int64, cb func(c Conn)) {
	if cb == nil {
		return
	}

	elidx := int(connId >> 48 & 0xffff)
	id := uint16(connId >> 32 & 0xffff)
	fd := int(connId & 0xffffffff)

	eng.lb.iterate(func(i int, el *eventloop) bool {
		if i == elidx {
			_ = el.poller.Trigger(func(_ interface{}) error {
				if c, ok := el.connections[fd]; ok && id == c.id {
					if c.opened {
						cb(c)
					}
				}
				return nil
			}, nil)
			return false
		}
		return true
	})
}

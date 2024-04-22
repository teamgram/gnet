// Copyright (c) 2019 The Gnet Authors. All rights reserved.
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

//go:build linux && !poll_opt
// +build linux,!poll_opt

package gnet

import (
	"errors"
	"runtime"

	"github.com/panjf2000/gnet/v2/internal/netpoll"
	errorx "github.com/panjf2000/gnet/v2/pkg/errors"
)

func (el *eventloop) rotate() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling(el.accept0)
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debugf("main reactor is exiting in terms of the demand from user, %v", err)
		err = nil
	} else if err != nil {
		el.getLogger().Errorf("main reactor is exiting due to error: %v", err)
	}

	el.engine.shutdown(err)

	return err
}

func (el *eventloop) orbit() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling(func(fd int, ev netpoll.IOEvent, flags netpoll.IOFlags) error {
		c := el.connections.getConn(fd)
		if c == nil {
			// Somehow epoll notified with an event for a stale fd that is not in our connection set.
			// We need to delete it from the epoll set.
			return el.poller.Delete(fd)
		}
		return c.processIO(fd, ev, flags)
	})
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debugf("event-loop(%d) is exiting in terms of the demand from user, %v", el.idx, err)
		err = nil
	} else if err != nil {
		el.getLogger().Errorf("event-loop(%d) is exiting due to error: %v", el.idx, err)
	}

	el.closeConns()
	el.engine.shutdown(err)

	return err
}

func (el *eventloop) run() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling(func(fd int, ev netpoll.IOEvent, flags netpoll.IOFlags) error {
		c := el.connections.getConn(fd)
		if c == nil {
			if _, ok := el.listeners[fd]; ok {
				return el.accept(fd, ev, flags)
			}
			// Somehow epoll notified with an event for a stale fd that is not in our connection set.
			// We need to delete it from the epoll set.
			return el.poller.Delete(fd)
		}
		return c.processIO(fd, ev, flags)
	})
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debugf("event-loop(%d) is exiting in terms of the demand from user, %v", el.idx, err)
		err = nil
	} else if err != nil {
		el.getLogger().Errorf("event-loop(%d) is exiting due to error: %v", el.idx, err)
	}

	el.closeConns()
	el.engine.shutdown(err)

	return err
}

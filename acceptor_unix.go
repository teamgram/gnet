// Copyright (c) 2021 Andy Pan
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
	"os"
	"time"

	"golang.org/x/sys/unix"

	"github.com/panjf2000/gnet/internal/netpoll"
	"github.com/panjf2000/gnet/internal/socket"
	"github.com/panjf2000/gnet/pkg/errors"
	"github.com/panjf2000/gnet/pkg/logging"
)

func (svr *server) accept(fd int, _ int, _ netpoll.IOEvent) error {
	for _, ln := range svr.lns {
		if fd == ln.fd {
			nfd, sa, err := unix.Accept(fd)
			if err != nil {
				if err == unix.EAGAIN {
					return nil
				}
				svr.opts.Logger.Errorf("Accept() fails due to error: %v", err)
				return errors.ErrAcceptSocket
			}
			if err = os.NewSyscallError("fcntl nonblock", unix.SetNonblock(nfd, true)); err != nil {
				return err
			}

			remoteAddr := socket.SockaddrToTCPOrUnixAddr(sa)
			if svr.opts.TCPKeepAlive > 0 && ln.network == "tcp" {
				err = socket.SetKeepAlive(nfd, int(svr.opts.TCPKeepAlive/time.Second))
				logging.Error(err)
			}

			el := svr.lb.next(remoteAddr)
			c := newTCPConn(nfd, el, sa, svr.opts.Codec.Clone(), ln.addr, remoteAddr)

			err = el.poller.UrgentTrigger(el.register, c)
			if err != nil {
				_ = unix.Close(nfd)
				c.releaseTCP()
			}
			return nil
		}
	}
	return nil
}

func (el *eventloop) accept(fd int, _ int, ev netpoll.IOEvent) error {
	for i, ln := range el.lns {
		if fd == ln.fd {
			if ln.network == "udp" {
				return el.readUDP(fd, i, ev)
			}

			nfd, sa, err := unix.Accept(ln.fd)
			if err != nil {
				if err == unix.EAGAIN {
					return nil
				}
				el.getLogger().Errorf("Accept() fails due to error: %v", err)
				return os.NewSyscallError("accept", err)
			}
			if err = os.NewSyscallError("fcntl nonblock", unix.SetNonblock(nfd, true)); err != nil {
				return err
			}

			remoteAddr := socket.SockaddrToTCPOrUnixAddr(sa)
			if el.svr.opts.TCPKeepAlive > 0 && ln.network == "tcp" {
				err = socket.SetKeepAlive(nfd, int(el.svr.opts.TCPKeepAlive/time.Second))
				logging.Error(err)
			}

			c := newTCPConn(nfd, el, sa, el.svr.opts.Codec.Clone(), ln.addr, remoteAddr)
			if err = el.poller.AddRead(c.pollAttachment); err != nil {
				return err
			}
			el.connections[c.fd] = c
			return el.open(c)
		}
	}
	return nil
}

//func (svr *server) acceptNewConnection(fd int, _ netpoll.IOEvent) error {
//	for i, ln := range svr.lns {
//		if fd == ln.fd {
//			nfd, sa, err := unix.Accept(ln.fd)
//			if err != nil {
//				if err == unix.EAGAIN {
//					return nil
//				}
//				svr.opts.Logger.Errorf("Accept() fails due to error: %v", err)
//				return errors.ErrAcceptSocket
//			}
//			if err = os.NewSyscallError("fcntl nonblock", unix.SetNonblock(nfd, true)); err != nil {
//				return err
//			}
//
//			netAddr := socket.SockaddrToTCPOrUnixAddr(sa)
//			if svr.opts.TCPKeepAlive > 0 && ln.network == "tcp" {
//				err = socket.SetKeepAlive(nfd, int(svr.opts.TCPKeepAlive/time.Second))
//				logging.LogErr(err)
//			}
//
//			el := svr.lb.next(netAddr)
//			c := newTCPConn(nfd, i, el, sa, netAddr)
//
//			err = el.poller.UrgentTrigger(el.loopRegister, c)
//			if err != nil {
//				_ = unix.Close(nfd)
//				c.releaseTCP()
//			}
//			return nil
//		}
//	}
//	return nil
//}
//
//func (el *eventloop) loopAccept(fd int, _ netpoll.IOEvent) error {
//	for i, ln := range el.lns {
//		if fd == ln.fd {
//			if ln.network == "udp" {
//				return el.loopReadUDP(ln.fd, i)
//			}
//
//			nfd, sa, err := unix.Accept(ln.fd)
//			if err != nil {
//				if err == unix.EAGAIN {
//					return nil
//				}
//				el.getLogger().Errorf("Accept() fails due to error: %v", err)
//				return os.NewSyscallError("accept", err)
//			}
//			if err = os.NewSyscallError("fcntl nonblock", unix.SetNonblock(nfd, true)); err != nil {
//				return err
//			}
//
//			netAddr := socket.SockaddrToTCPOrUnixAddr(sa)
//			if el.svr.opts.TCPKeepAlive > 0 && ln.network == "tcp" {
//				err = socket.SetKeepAlive(nfd, int(el.svr.opts.TCPKeepAlive/time.Second))
//				logging.LogErr(err)
//			}
//
//			c := newTCPConn(nfd, i, el, sa, netAddr)
//			if err = el.poller.AddRead(c.pollAttachment); err == nil {
//				el.connections[c.fd] = c
//				return el.loopOpen(c)
//			}
//			return err
//		}
//	}
//	return nil
//}

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  bb.py
#
#  Copyright 2016 Jelle Smet <development@smetj.net>
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
#
#

from wishbone.event import Event
from wishbone.event import Bulk
from wishbone.actor import ActorConfig
from wishbone.utils.test import getter
from wishbone_output_tcp import TCPOut

from gevent.server import StreamServer
from gevent.queue import Queue
from gevent import spawn, sleep


class TCPServer():

    def __init__(self):
        self.q = Queue()
        self.tcp = StreamServer(('0.0.0.0', 19283), self.handle)

    def handle(self, socket, address):

        rfileobj = socket.makefile(mode='rb')
        while True:
            lines = rfileobj.readlines()
            if not lines:
                break
            else:
                self.q.put("".join(lines))
                break
        rfileobj.close()

    def start(self):
        spawn(self.tcp.start)

    def stop(self):
        self.tcp.stop()


def test_module_tcp_default():

    tcpserver = TCPServer()
    tcpserver.start()

    actor_config = ActorConfig('tcpout', 100, 1, {}, "")
    tcpout = TCPOut(actor_config)
    tcpout.pool.queue.inbox.disableFallThrough()
    tcpout.start()

    e = Event('this_is_a_test')

    tcpout.pool.queue.inbox.put(e)
    one = tcpserver.q.get()
    assert one == "this_is_a_test\n"
    tcpserver.stop()


def test_module_tcp_bulk():

    tcpserver = TCPServer()
    tcpserver.start()

    actor_config = ActorConfig('tcpout', 100, 1, {}, "")
    tcpout = TCPOut(actor_config)
    tcpout.pool.queue.inbox.disableFallThrough()
    tcpout.start()

    event_one = Event('this_is_a_test_1')
    event_two = Event('this_is_a_test_2')

    bulk_event = Bulk()
    bulk_event.append(event_one)
    bulk_event.append(event_two)

    tcpout.pool.queue.inbox.put(bulk_event)

    sleep(1)
    one = getter(tcpserver.q)
    assert one == "this_is_a_test_1\nthis_is_a_test_2\n"
    tcpserver.stop()

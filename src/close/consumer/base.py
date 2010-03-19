#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Abstract base classes for a gevent_ based Streaming API consumer:
  
  #. a long running streaming API ``Consumer`` that presumes chunked
     http encoding
  #. a ``BaseManager`` which looks after ``Consumer`` instances
     running in their own ``gevent.Greenlet`` thread
  #. an ultra-simple WSGI app to recieve ``/stop``, ``/start`` and 
     ``/restart`` instructions
  
  See ``consumer.py`` for a specific implementation.
  
  .. _gevent: http://www.gevent.org/
"""

import gevent

from gevent import monkey
monkey.patch_all()

from gevent import queue
notification_queue = queue.Queue()

from gevent import sleep, socket

import cgi
import logging
import httplib

from utils import generate_hash, generate_auth_header, unicode_urlencode

class ChunkReadingMixin(object):
    """Implements chunked http reading ala
      ``httplib.HTTPResponse._read_chunked``.
      
      Use ``self._read_chunked(l)`` instead of ``self.sock.recv(l)``
      and the fact that we're consuming a raw HTTP chunk transfer 
      encoded stream is dissolved.
    """
    
    sock = NotImplemented
    chunk_left = None
    
    def _safe_read(self, amt):
        s = []
        while amt > 0:
            chunk = self.sock.recv(amt)
            if not chunk:
                raise gevent.GreenletExit('Connection closed')
            s.append(chunk)
            amt -= len(chunk)
        return ''.join(s)
        
    
    def _safe_readline(self):
        s = []
        while True:
            c = self.sock.recv(1)
            if c == '\n':
                break
            elif not c:
                raise gevent.GreenletExit('Connection closed by server')
            s.append(c)
        return ''.join(s).strip()
        
    
    
    def _read_chunked(self, amt):
        chunk_left = self.chunk_left
        value = []
        while True:
            if chunk_left is None:
                line = self._safe_readline()
                try:
                    chunk_left = int(line, 16)
                except ValueError:
                    raise gevent.GreenletExit('Lost chunk sync')
                if chunk_left == 0:
                    return ''.join(value)
            if amt is None:
                value.append(self._safe_read(chunk_left))
            elif amt < chunk_left:
                value.append(self._safe_read(amt))
                self.chunk_left = chunk_left - amt
                return ''.join(value)
            elif amt == chunk_left:
                value.append(self._safe_read(amt))
                self._safe_read(2)  # toss the CRLF at the end of the chunk
                self.chunk_left = None
                return ''.join(value)
            else:
                value.append(self._safe_read(chunk_left))
                amt -= chunk_left
            # we read the whole chunk, get another
            self._safe_read(2) # toss the CRLF at the end of the chunk
            chunk_left = None
        
    
    def _readline_chunked(self):
        chars = []
        while True:
            c = self._read_chunked(1)
            if c == '\n':
                break
            elif not c:
                raise gevent.GreenletExit('Connection closed by server')
            chars.append(c)
        return ''.join(chars).strip()
        
    
    


class BaseConsumer(ChunkReadingMixin):
    """Connect to the Streaming API and put data into the queue.
    """
    
    sock = None
    
    def __init__(
            self, host, path, port=80, params={}, headers={},
            timeout=61, username=None, password=None, 
            min_tcp_ip_delay=0.25, max_tcp_ip_delay=16,
            min_http_delay=10, max_http_delay=240
        ):
        """Store config and build the connection headers.
        """
        
        self.host = host
        self.port = port
        self.path = path
        self.body = unicode_urlencode(params)
        if username and password:
            headers['Authorization'] = generate_auth_header(username, password)
        header_lines = [
            'POST %s HTTP/1.1' % self.path,
            'Host: %s' % self.host,
            'Content-Length: %s' % len(self.body),
            'Content-Type: application/x-www-form-urlencoded'
        ]
        header_lines.extend([
                '%s: %s' % (k, v) for k, v in headers.iteritems()
            ] + ['', '']
        )
        self.headers = '\r\n'.join(header_lines)
        self.timeout = timeout
        self.min_tcp_ip_delay = min_tcp_ip_delay
        self.max_tcp_ip_delay = max_tcp_ip_delay
        self.min_http_delay = min_http_delay
        self.max_http_delay = max_http_delay
        self.id = generate_hash()
        
    
    
    def _incr_tcp_ip_delay(self, delay):
        """When a network error (TCP/IP level) is encountered, 
          back off linearly.
        """
        
        min_ = self.min_tcp_ip_delay
        max_ = self.max_tcp_ip_delay
        
        delay += min_
        if delay > max_:
            delay = max_
        if delay == max_:
            logging.warning('Consumer reached max tcp ip delay')
        return delay
        
    
    def _incr_http_delay(self, delay):
        """When an http error (> 200) is returned, back off 
          exponentially.
          
              >>> d = _incr_http_delay(0)
              >>> d
              10
              >>> d = _incr_http_delay(d)
              >>> d
              30
              >>> d = _incr_http_delay(d)
              >>> d
              70
              >>> d = _incr_http_delay(d)
              >>> d
              150
              >>> d = _incr_http_delay(d)
              >>> d
              240
              >>> d = _incr_http_delay(d)
              >>> d
              240
          
        """
        
        min_ = self.min_http_delay
        max_ = self.max_http_delay
        
        delay = min_ + min_ * delay / 5
        if delay > max_:
            delay = max_
        if delay == max_:
            logging.warning('Consumer reached max http delay')
        return delay
        
    
    
    def _notify(self, event_name, data):
        """Puts an {event_name: data} item into a gevent queue_
          
          .. _queue: http://www.gevent.org/gevent.queue.html
        """
        
        item = {}
        item[event_name] = data
        notification_queue.put_nowait(item)
        
    
    def _consume_stream(self):
        """Consume the stream ad infinitum.
        """
        
        while True:
            data = self.get_data()
            if data:
                self._notify('data', data)
            
        
    
    
    def _get_status_and_consume_headers(self):
        """When the connection is first made, parse out the status
          and chew through the remaining headers.
        """
        
        # read the status
        line = self._safe_readline()
        status = int(line.strip().split(' ')[1])
        # exhaust the headers
        while True:
            line = self._safe_readline()
            if not line:
                break
        return status
        
    
    def get_data(self):
        """Overwrite to read data.
        """
        
        raise NotImplementedError
        
    
    
    def run(self):
        tcp_ip_delay = 0
        http_delay = 0
        notify_on_exit = True
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.sock.settimeout(self.timeout)
                self.sock.connect((self.host, self.port))
                self.sock.send(self.headers)
                self.sock.send(self.body)
                status = self._get_status_and_consume_headers()
                if status == 200:
                    self._notify('connect', self.id)
                    tcp_ip_delay = 0
                    http_delay = 0
                    self._consume_stream()
                else:
                    self.sock.close()
                    if status > 500:
                        http_delay = self._incr_http_delay(http_delay)
                        sleep(http_delay)
                    else: # we're doing something wrong
                        logging.warning(status)
                        notify_on_exit = False
                        break
            except (socket.timeout, socket.error), err:
                logging.info(err, exc_info=True)
                self.sock.close()
                tcp_ip_delay = self._incr_tcp_ip_delay(tcp_ip_delay)
                sleep(tcp_ip_delay)
            except gevent.GreenletExit:
                self.sock.close()
                notify_on_exit = False
                break
            except Exception, err:
                logging.warning('Fatal exception in Consumer')
                logging.warning(err, exc_info=True)
                self.sock.close()
                break
        if notify_on_exit:
            self._notify('exit', self.id)
        
        
    
    


class BaseManager(object):
    """Manages running consumer instances in their own greenlet.
      
      Handles low latency restarts and unexpected errors.  To use,
      override ``get_params`` and ``handle_data`` and pass an 
      instance of your implementation to a subclass of 
      ``BaseWSGIApp``'s constructor.
    """
    
    # we keep a dictionary of consumers, using the consumer.id
    # as the dictionary key and the greenlet they're running in
    # as the value
    consumers = {}
    active_consumer_id = None
    
    # we back off from repeated unexpected exits
    exit_delay = 0
    
    def __init__(
            self, consumer_class, host, path, username=None, password=None, 
            num_workers=10, min_exit_delay=0.25, max_exit_delay=16
        ):
        self.consumer_class = consumer_class
        self.host = host
        self.path = path
        self.username = username
        self.password = password
        self.min_exit_delay = min_exit_delay
        self.max_exit_delay = max_exit_delay
        # spawn worker greenlets to handle notifications
        for i in range(num_workers):
            gevent.spawn(self._handle_event)
        
    
    
    def _incr_exit_delay(self):
        """When a network error (TCP/IP level) is encountered, 
          back off linearly.
        """
        
        delay = self.exit_delay
        
        min_ = self.min_exit_delay
        max_ = self.max_exit_delay
        
        delay += min_
        
        if delay > max_:
            delay = max_
        if delay == max_:
            logging.warning('Manager reached max unexpected exit delay')
        
        self.exit_delay = delay
        
    
    
    def _handle_connect(self, consumer_id):
        """When a consumer connects successuflly, kill any other
          active consumers.
          
          This approach enables a low latecy restart, i.e.: only 
          kill the active consumer when the new one is actually
          up and running.
        """
        
        logging.info('handle_connect %s' % consumer_id)
        logging.info(self.consumers)
        
        self.exit_delay = 0
        self.active_consumer_id = consumer_id
        for k, v in self.consumers.items():
            if not k == consumer_id:
                logging.info('killing: %s' % k)
                v.kill(block=True)
                del self.consumers[k]
            
        
        
    
    def _handle_exit(self, consumer_id):
        """If exit wasn't scheduled, start again.
        """
        
        logging.info('handle_exit %s' % consumer_id)
        logging.info(self.consumers)
        
        # remove it from the dict of consumers we're manitaining
        del self.consumers[consumer_id]
        
        # if it exited unexpectedly
        self._incr_exit_delay()
        if consumer_id == self.active_consumer_id:
            logging.info('active consumer exited unexpectedly')
            gevent.spawn_later(self.exit_delay, self.start_a_consumer)
        elif not self.consumers:
            logging.info('only consumer exited unexpectedly')
            gevent.spawn_later(self.exit_delay, self.start_a_consumer)
        
    
    def _handle_data(self, data):
        """Overwrite to do something_ with the data.
          
          .. _something: http://bit.ly/2NDL32
        """
        
        self.handle_data(data)
        
    
    def _handle_event(self):
        while True:
            item = notification_queue.get()
            for k, v in item.iteritems():
                getattr(self, '_handle_%s' % k)(v)
            
        
    
    
    def start_a_consumer(self):
        """Fire up a new Consumer.
        """
        
        logging.info('creating new consumer')
        
        # create the new consumer
        consumer = self.consumer_class(
            path=self.path,
            host=self.host,
            params=self.get_params(),
            username=self.username, 
            password=self.password
        )
        logging.info(consumer.id)
        
        # start the consumer in a new greenlet
        g = gevent.spawn(consumer.run)
        
        # put it in self.consumers
        self.consumers[consumer.id] = g
        logging.info(self.consumers)
        
    
    def stop_all_consumers(self, accept_updates=True):
        """Kill any consumers.
        """
        
        self.active_consumer_id = None
        
        for item in self.consumers.itervalues():
            item.kill(block=True)
        
        self.consumers = {}
        
    
    
    def get_params(self):
        """Override to specify the parameters to POST to the streaming
          API when connecting.
        """
        
        raise NotImplementedError
        
    
    def handle_data(self, data):
        """Override to do something with the data.
        """
        
        raise NotImplementedError
        
    
    


class BaseWSGIApp(object):
    """Responds to requests to urls in ``/self.__all__``:
      
          from gevent import wsgi
          
          app = WSGIApp(manager=Manager())
          server = wsgi.WSGIServer(('', PORT), app.handle_requests)
          server.serve_forever()
          
      
      Override ``handle_request_params`` as necessary.
      
    """
    
    __all__ = [
        'start',
        'stop',
        'restart'
    ]
    
    def __init__(self, manager):
        self.manager = manager
        
    
    
    def _start(self):
        self.manager.start_a_consumer()
        
    
    def _stop(self):
        self.manager.stop_all_consumers()
        
    
    def _restart(self):
        self._stop()
        self._start()
        
    
    
    def handle_requests(self, env, start_response):
        action = env['PATH_INFO'].replace('/', '')
        if action in self.__all__:
            start_response('200 OK', [('Content-Type', 'text/plain')])
            params = {}
            body = env['wsgi.input'].read()
            for name, values in cgi.parse_qs(body).iteritems():
                l = []
                l.extend(values)
                params[name] = l
            self.handle_request_params(action, params)
            getattr(self, '_%s' % action)()
            return ["OK\r\n"]
        else:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['Not Found\r\n']
        
    
    
    def handle_request_params(self, action, params):
        """The idea here is that, when you communicate with the
          manager, you can pass info in the params.
        """
        
        raise NotImplementedError
        
    
    
    
    


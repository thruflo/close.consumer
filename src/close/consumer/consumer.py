#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A specific implementation, using redis_ for persistence.
  
  .. _redis: http://code.google.com/p/redis/
"""

from base import BaseConsumer, BaseManager, BaseWSGIApp

import logging

import gevent

from redis import Redis
r = Redis()

NAMESPACE = u'close.consumer.'
FOLLOW_KEY = u'%sfollow' % NAMESPACE
TRACK_KEY = u'%strack' % NAMESPACE
DATA_KEY = u'%sdata' % NAMESPACE
NOTIFICATION_KEY = u'%snotify' % NAMESPACE

class Consumer(BaseConsumer):
    """Gets data delimited_ by length.
      
      .. _delimited: http://apiwiki.twitter.com/Streaming-API-Documentation#delimited
    """
    
    def get_data(self):
        line = self._readline_chunked()
        if line.isdigit():
            return self._read_chunked(int(line))
        
    
    


class Manager(BaseManager):
    """Generate the filter predicates and handle the data.
    """
    
    def get_params(self):
        """Get the predicates from redis.
        """
        
        params = {}
        follow = r.get(FOLLOW_KEY)
        track = r.get(TRACK_KEY)
        if follow:
            params['follow'] = follow
        if track:
            params['track'] = track
        return params
        
    
    def handle_data(self, data):
        """Append the data to a redis list and notify that 
          we've done so.
        """
        
        r.rpush(DATA_KEY, data)
        r.rpush(NOTIFICATION_KEY, 1)
        
    
    


class WSGIApp(BaseWSGIApp):
    def handle_request_params(self, action, params):
        """Save the predicates.
        """
        
        logging.warning(
            '@@ no validation is being applied to follow and track params'
        )
        
        follow = params.get('follow', None)
        track = params.get('track', None)
        if follow is not None:
            r.set(FOLLOW_KEY, ','.join(map(str, follow)))
        if track is not None:
            r.set(TRACK_KEY, ','.join(map(str, track)))
        
        
    
    


def parse_options():
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option(
        '--logging',
        dest='log_level',
        action='store',
        type='string',
        default='info'
    )
    parser.add_option(
        '--host', 
        dest='host',
        action='store', 
        type='string',
        help='the host you want the streaming API ``Consumer`` to connect to',
        default='stream.twitter.com'
    )
    parser.add_option(
        '--path', 
        dest='path',
        action='store', 
        type='string',
        help='the path you want the streaming API ``Consumer.conn`` to request',
        default='/1/statuses/filter.json?delimited=length'
    )
    parser.add_option(
        '--username',
        dest='username',
        action='store',
        type='string',
        help='the basic http auth username you want to use, if any',
        default=''
    )
    parser.add_option(
        '--password',
        dest='password',
        action='store',
        type='string',
        help='the basic http auth password you want to use, if any',
        default=''
    )
    parser.add_option(
        '--port',
        dest='port',
        action='store',
        type='int',
        help='the local port you want to expose the ``WSGIApp`` on',
        default=8282
    )
    parser.add_option(
        '--serve-and-start',
        dest='should_start_consumer',
        action='store_true', 
        help='start a consumer by default',
        default=True
    )
    parser.add_option(
        '--serve-only',
        dest='should_start_consumer',
        action='store_false', 
        help='don\'t start a consumer by default'
    )
    return parser.parse_args()[0]
    

def main():
    from gevent import wsgi
    
    options = parse_options()
    logging.basicConfig(
        level=getattr(
            logging, 
            options.log_level.upper()
        )
    )
    
    kwargs = {}
    if options.username:
        kwargs['username'] = options.username
    if options.password:
        kwargs['password'] = options.password
    
    manager = Manager(Consumer, options.host, options.path, **kwargs)
    if options.should_start_consumer:
        manager.start_a_consumer()
    
    app = WSGIApp(manager=manager)
    server = wsgi.WSGIServer(('', options.port), app.handle_requests)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    
    


if __name__ == '__main__':
    main()
    


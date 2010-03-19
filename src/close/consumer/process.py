#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Implementation of a queue processor.  When there are ``num_items`` in
  the queue, dispatches them elsewhere.
  
  @@ todo: this could be a base class and an implementation, with
  ``self.handle_batch()`` rather than ``self._post()``.
"""

import gevent
from gevent import sleep

from consumer import r, DATA_KEY, NOTIFICATION_KEY

import logging
import urllib2

from utils import generate_auth_header, unicode_urlencode

class PostingParsingQueueProcessor(object):
    """Uses redis' `blocking pop command`_ to accept notifications
      on ``NOTIFICATION_KEY``.  If there are ``self.num_items`` in 
      ``DATA_KEY``, moves the items into a ``self.ready_key`` list, 
      optionally parses the items and posts them off to a url provided.
      
      You can run multiple processor instances, as long as you pass them
      different ``ready_list_id``s via ``--ready-list-id=...``.
      
      .. _`blocking pop command`: http://code.google.com/p/redis/wiki/BlpopCommand
    """
    
    def __init__(
            self, ready_list_id, num_items, url, headers={}, username=None, password=None,
            item_parser=None, min_sleep=2, max_sleep=3600
        ):
        self.ready_key = '%s.%s' % (DATA_KEY, ready_list_id)
        self.num_items = num_items
        self.url = url
        if username and password:
            headers['Authorization'] = generate_auth_header(username, password)
        self.headers = headers
        self.item_parser = item_parser
        self.delay = min_sleep
        self.min_sleep = min_sleep
        self.max_sleep = max_sleep
        
    
    
    def _incr_delay(self):
        """Increments the delay exponentially until it
          reaches ``self.max_sleep``.
        """
        
        if self.delay < self.max_sleep:
            self.delay += self.delay
            if self.delay > self.max_sleep:
                self.delay = self.max_sleep
            
        
    
    def _reset_delay(self):
        """Resets the delay to ``self.min_sleep``.
        """
        
        if self.delay > self.min_sleep:
            self.delay = self.min_sleep
            
        
    
    
    def _post(self, items):
        data = unicode_urlencode({'items': items})
        request = urllib2.Request(
            self.url, 
            data=data,
            headers=self.headers
        )
        status = urllib2.urlopen(request).getcode()
        logging.debug(status)
        return 200 <= status < 300
        
    
    def _parse(self, items):
        """If we've been provided with an ``item_parser`` function, use
          it to parse the item.
        """
        
        if not self.item_parser:
            for item in items:
                yield item
        else:
            for item in items:
                parsed_item = self.item_parser(item)
                if parsed_item:
                    yield parsed_item
                
            
        
    
    
    def loop_forever(self):
        logging.info('starting to loop forever')
        READY_KEY = self.ready_key
        NUM_ITEMS = self.num_items
        while True:
            logging.debug('.')
            should_post = True
            # if there's nothing in READY_KEY
            if not r.llen(READY_KEY):
                # block waiting for notifications
                logging.debug('blocking waiting for %s' % NOTIFICATION_KEY)
                r.blpop([NOTIFICATION_KEY])
                # on notification, if there are num_items in the data list
                n = r.llen(DATA_KEY)
                logging.debug(n)
                if n < NUM_ITEMS:
                    should_post = False
                else:
                    # move them into READY_KEY, neatly clearing DATA_KEY
                    # in the same fell swoop
                    r.rename(DATA_KEY, READY_KEY)
            if should_post:
                # read the items from READY_KEY
                items = r.lrange(READY_KEY, 0, -1)
                logging.debug(items)
                # try to post them off
                success = self._post(self._parse(items))
                logging.debug(success)
                if success:
                    self._reset_delay()
                    r.delete(READY_KEY)
                else: 
                    # deliberately leave the items in READY_KEY
                    self._incr_delay()
                    sleep(self.delay)
                
            
        
        
    
    


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
        '--ready-list-id',
        dest='ready_list_id',
        action='store',
        type='string',
        default='ready'
    )
    parser.add_option(
        '--num-items',
        dest='num_items',
        action='store',
        type='int',
        default=10
    )
    parser.add_option(
        '--url',
        dest='url',
        action='store',
        type='string',
        default='https://closeapp.appspot.com/hooks/handle_status'
    )
    parser.add_option(
        '--username',
        dest='username',
        action='store',
        type='string',
        default=''
    )
    parser.add_option(
        '--password',
        dest='password',
        action='store',
        type='string',
        default=''
    )
    return parser.parse_args()[0]

def main():
    from parse import parse_item
    
    options = parse_options()
    logging.basicConfig(
        level=getattr(
            logging, 
            options.log_level.upper()
        )
    )
    
    processor = PostingParsingQueueProcessor(
        options.ready_list_id,
        options.num_items, 
        options.url, 
        username=options.username, 
        password=options.password,
        item_parser=parse_item
    )
    
    try:
        processor.loop_forever()
    except KeyboardInterrupt:
        pass
    
    


if __name__ == '__main__':
    main()



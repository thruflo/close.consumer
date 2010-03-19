#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Implementation of an item parser.  
  
  ``parse_item`` should be passed into the 
  ``process.PostingParsingQueueProcessor`` constructor, ala:
  
      p = PostingParsingQueueProcessor(item_parser=parse_item)
  
"""

import logging

try:
    import simplejson as json
except ImportError:
    try:
        import json
    except ImportError:
        try:
            from django.utils import simplejson as json
        except ImportError:
            raise ImportError, "Can't load a json library"
        
    
def _json_decode(value):
    if isinstance(value, str):
        return value.decode("utf-8")
    assert isinstance(value, unicode)
    return json.loads(value)
    


RELEVANT_KEYS = (
    'id',
    'in_reply_to_status_id',
    'in_reply_to_user_id',
    'retweeted_status',
    'text',
    'user'
)
def parse_item(item):
    """Reduce a status to just the important bits.
    """
    
    try:
        data = _json_decode(item)
    except ValueError, err:
        logging.warning('not a valid json string')
        logging.warning(s)
    else:
        if data.has_key('in_reply_to_status_id'):
            # assume it's a bonefide status update
            # strip down to just the keys we're interested in
            d2 = {}
            for k in RELEVANT_KEYS:
                v = data[k]
                # special casing 'retweeted_status' and 'user'
                # to select just the relevant bits of their
                # data if provided
                if k == 'retweeted_status':
                    d2[k] = v and {'id': v['id']} or v
                elif k == 'user':
                    d2[k] = v and {'id': v['id'], 'screen_name': v['screen_name']} or v
                else:
                    d2[k] = v
            return json.dumps(d2)
        elif data.has_key('delete') or data.has_key('limit'):
            # assume it's a deletion or limitation notice
            return item
        
    


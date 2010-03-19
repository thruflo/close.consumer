#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Helper functions.
"""

import base64
import hashlib
import random
import time
import urllib

def unicode_urlencode(params):
    if isinstance(params, dict):
        params = params.items()
    return urllib.urlencode([(
                k, 
                isinstance(v, unicode) and v.encode('utf-8') or v
            ) for k, v in params
        ]
    )


def generate_hash(algorithm='sha1', s=None):
    """Generates a random string.
    """
    
    # if a string has been provided use it, otherwise default 
    # to producing a random string
    s = s is None and '%s%s' % (random.random(), time.time()) or s
    hasher = getattr(hashlib, algorithm)
    return hasher(s).hexdigest()
    


def generate_auth_header(username, password):
    auth = base64.b64encode(u'%s:%s' % (username, password))
    return 'Basic %s' % auth



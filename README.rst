Abstract base classes for a long running, streaming API consumer and a specific implementation that puts the data into a redis_ queue and processes it in batches.

Requires gevent_.  The example implementation depends on redis_.

Coded initially to send batches of statuses from the `Twitter Streaming API`_ to an appengine webhook.  May (or may not) be usable for other purposes.

Presumes the data coming down the pipe is `Chunked Transfer Encoded`_.

.. _redis: http://code.google.com/p/redis/
.. _gevent: http://www.gevent.org/
.. _Twitter Streaming API: http://apiwiki.twitter.com/Streaming-API-Documentation
.. _Chunked Transfer Encoded: http://en.wikipedia.org/wiki/Chunked_transfer_encoding
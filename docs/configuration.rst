Configuration
=============

When initializing H3, through the ``H3_Init()`` function (or similar in Python and Java), you need to specify a storage URI to select the storage backend type and options.

Example storage URIs include (defaults for each type shown):

* ``file:///tmp/h3`` for local filesystem
* ``kreon://127.0.0.1:2181`` for Kreon, where the network location refers to the ZooKeeper host and port
* ``rocksdb:///tmp/h3/rocksdb`` for `RocksDB <https://rocksdb.org>`_
* ``redis://127.0.0.1:6379`` for `Redis <https://redis.io>`_

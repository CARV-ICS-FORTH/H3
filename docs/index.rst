.. H3 documentation master file, created by
   sphinx-quickstart on Thu Nov 28 11:35:06 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to H3
=============

H3 is an embedded High speed, High volume, and High availability object store, backed by a high-performance key-value store.
H3 is implemented in the ``h3lib`` library, which provides a cloud-friendly API, similar to Amazon's S3. On the backend, H3 supports `RocksDB <https://rocksdb.org>`_ for single-node runs, `Redis <https://redis.io>`_ for over-the-network storage, Kreon for distributed storage deployments, as well as plain files for layering on top of any filesystem (and easy testing).

``h3lib`` is written in C. Python and Java wrappers are also available. An H3 FUSE-based filesystem allows object access using file semantics.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   design
   configuration
   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

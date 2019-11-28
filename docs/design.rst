Design
======

H3 is a thin, stateless layer that provides a cloud-aware application object semantics on top of a high-performance key-value store - a typical service deployed in HPC and Big Data environments. By transitioning to H3 and running in a cluster, we expect applications to enjoy much faster data operations and - if the key-value store is distributed - to easily scale out accross all cluster nodes. In the later case, the object service is not provided centrally, but *everywhere* on the cluster.

Overview
--------

H3 provides a flat organization scheme where each data object is linked to a globally unique identifier called bucket. Buckets contain objects. The H3 API supports typical bucket operations, such as create, list, and delete. Object management includes reading/writing objects from/in H3, copying, renaming, listing, and deleting. H3 supports multipart operations, where objects are written in parts and are coalesced at the end. Additionally, it provides a file-stream API, enabling access to H3 objects using typical file-like function calls.

In essence, H3 implements a translation layer between the object namespace and a key-value store, similar to how a filesystem provides a hierarchical namespace of files on top of a block device. However, there are major differences:

- The object namespace is *pseudo hierarchical*, meaning there is no real hierarchy imposed. Object names can contain ``/`` as a delimiter and the list operation supports a prefix parameter to return all respective objects; like issuing a ``find <path>`` command in a filesystem, but not an ``ls <path>``.
- Buckets can only include data files, not special files, like links, sockets, etc.
- The key-value store provides a much richer set of data query and manipulation primitives, in contrast to a typical block device. It can handle arbitrary value sizes, scan keys (return all keys starting with a prefix), operate on multiple keys in a single transaction, etc. H3 takes advantage of those primitives in order to minimize code complexity and exploit any optimizations done in the key-value layer.

H3 is provided as C library, called ``h3lib``. ``h3lib`` implements the object API as a series of functions that translate the bucket and object operations to operations in the provided key-value store (backend). The key-value store interface is abstracted into another library, ``kvlib``, which has implementations for `Redis <https://redis.io>`_, `RocksDB <https://rocksdb.org>`_ and - our own sister project - Kreon.

The ``h3lib`` API is outlined here.

[Provide internal link to ``h3lib`` API documentation generated from docstrings]

Data organization
-----------------

Buckets and objects in H3 have associated metadata. Objects also have data, that - depending on the creation/write method - can consist of a single or multiple parts. We assume that the key-value backend may have size limits on either keys and/or values.

[Authentication]

[List buckets per user]

Metadata
^^^^^^^^

Each bucket and each object in H3 has a unique name identifier which corresponds to a key in the backend holding its metadata. Object keys are produced by concatenating the bucket name, ``/`` and the full object name (eg. ``mybucket/a``, ``mybucket/a/b/c``, or ``mybucket/a|b|c``).

Bucket metadata includes:

* Creation time

Object metadata includes:

* Creation time
* Access time (last read or write)
* Modification time (last write)
* Size in bytes for each data part

By storing bucket/object names as keys, we are able to use the key-value's scan operation to implement object listings. If the backend has limited key size, a respective limitation applies to object name lengths. We expect renaming an object to be handled optimally by the backend (not with a copy and delete of the value).

To create a bucket or object, we query for an existing key first to avoid conflicts.

We do not allow deleting an non-empty bucket (it has to be emptied first).

Data
^^^^

In the simplest case, where object data is smaller than the value size limitation, we just store the data under a key corresponding to the respective name identifier's hash (eg. data for ``mybucket/a`` goes into key ``hash('mybucket/a')``). The hash function used is SHA-256, which produces a 256-bit (random) key for each object name.

Object data is broken up into parts if it is larger than the value size limitation, or is provided as such by the user via the multipart API. Part ``i`` of data belonging to ``mybucket/a`` goes into key ``hash('mybucket/a') + '#' + i``. Parts provided by the user that exceed our maximum value size can themselves be broken into internal parts, thus it is possible to have a second level of data segmentation, encoded in keys as ``hash('mybucket/a') + '#' + i + '#' + j``

[Multipart create]


*Note: There has been a discussion on splitting up data into extents and storing the extents as content-hashed blocks. This has pros (fast copies, data deduplication, snapshots) and cons (hash lists in metadata management, hash calculation, garbage collection).*




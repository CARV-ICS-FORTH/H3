Design
======

H3 is a thin, stateless layer that provides a cloud-aware application object semantics on top of a high-performance key-value store - a typical service deployed in HPC and Big Data environments. By transitioning to H3 and running in a cluster, we expect applications to enjoy much faster data operations and - if the key-value store is distributed - to easily scale out accross all cluster nodes. In the later case, the object service is not provided centrally, but *everywhere* on the cluster.

Overview
--------

H3 provides a flat organization scheme where each data object is linked to a globally unique identifier called bucket. Buckets belong to users and contain objects. The H3 API supports typical bucket operations, such as create, list, and delete. Object management includes reading/writing objects from/to H3, copying, renaming, listing, and deleting. H3 also supports multipart operations, where objects are written in parts and coalesced at the end. [Additionally, it provides a file-stream API, enabling access to H3 objects using typical file-like function calls.]

In essence, H3 implements a translation layer between the object namespace and a key-value store, similar to how a filesystem provides a hierarchical namespace of files on top of a block device. However, there are major differences:

- The object namespace is *pseudo hierarchical*, meaning there is no real hierarchy imposed. Object names can contain ``/`` as a delimiter and the list operation supports a prefix parameter to return all respective objects; like issuing a ``find <path>`` command in a filesystem, but not an ``ls <path>``.
- Buckets can only include data files, not special files, like links, sockets, etc.
- The key-value store provides a much richer set of data query and manipulation primitives, in contrast to a typical block device. It can handle arbitrary value sizes, scan keys (return all keys starting with a prefix), operate on multiple keys in a single transaction, etc. H3 takes advantage of those primitives in order to minimize code complexity and exploit any optimizations done in the key-value layer.

H3 is provided as C library, called ``h3lib``. ``h3lib`` implements the object API as a series of functions that translate the bucket and object operations to operations in the provided key-value backend. The key-value store interface is abstracted into another library, ``kvlib``, which has implementations for `Redis <https://redis.io>`_, `RocksDB <https://rocksdb.org>`_ and - our own sister project - Kreon.

The ``h3lib`` API is outlined here. [Provide internal link to ``h3lib`` API documentation generated from docstrings.]

User management
---------------

H3 maps user credentials, given at library initialization, to a user id with the help of an external service [TBD]. The user id is passed along in all H3 calls and stored in bucket/object metadata when they are created. Bucket/object operations fail if the provided user id does not match the respective id stored for the specific entity. [Elaborate authorization policies are planned with per-bucket/object ACLs.]

Note that H3 cannot enforce strict data protection semantics, due to its nature - being a library layered on top of a key-value store and not a REST service. Complete data isolation between users can really only be accomplished either by mapping H3 users to backend domains of some kind (assuming the key-value store supports it), or by using separate key-value deployments/namespaces per user.

Data organization
-----------------

Users, buckets and objects in H3 have associated metadata. Objects also have data, that - depending on the creation/write method - can consist of a single or multiple parts of variable size. We assume that the key-value backend may have size limits on either keys and/or values thus H3 will automatically break over-sized values into an appropriate number of smaller parts.

Metadata
^^^^^^^^

Each user, bucket and object in H3 has a unique name identifier corresponding to a key in the backend holding its metadata. The object metadata contain -among others- the keys corresponding to the data. User keys are formulated by concatenating ``'@'`` and the user id (eg. ``@42``). Bucket keys are the bucket names. Object keys are produced by concatenating the bucket name, ``/`` and the full object name (eg. ``mybucket/a``, ``mybucket/a/b/c``).

**Valid characters**
Bucket ID:  The bucket name may be made up of all alaphanumeric characters except ``/`` since this the bucket-object delimeter.
Object ID: TBD
User ID: TBD

User metadata includes:

* List of buckets

Bucket metadata includes:

* User id
* Creation time

Object metadata includes:

* User id
* Creation time
* Access time (last read or write)
* Modification time (last write)
* Size in bytes for each data part

By storing bucket/object names as keys, we are able to use the key-value's scan operation to implement object listings. We expect the backend to provide an option to disallow overwriting keys if they already exist (using a ``create()`` function instead of ``put()``), to avoid race conditions when creating resources. If the backend has a limited key size, a respective limitation applies to name identifier lengths. We also assume renaming an object to be handled optimally by the backend (not with a copy and delete of the value).

To avoid resizing values for object metadata very often, we allocate metadata in duplicates of a batch size, where each batch may hold information for several data parts. The same applies to user metadata for storing bucket names.

We handle multipart data writes (multipart upload) with special types of objects, which exist in the namespace that results from concatenating the bucket name with marker ``$`` and an identifier (upload_id) generated internally on user request. The id is the hash of the bucket and object-id concatenation. 
Data
^^^^

In the simplest case, where object data is smaller than the value size limitation, we just store the data under a key corresponding to the string representation of the respective name identifier's hash (eg. data for ``mybucket/a`` goes into key ``'_' + hash('mybucket/a')``). The hash function used is SHA-256, which produces a 32-byte (random) key for each object name. The underscore is added at the beginning to ensure that data keys are not intermixed with bucket/object name keys during scan operations.

Object data is broken up into parts if it is larger than the value size limitation, or is provided as such by the user via the multipart API. Part ``i`` of data belonging to ``mybucket/a`` goes into key ``'_' + hash('mybucket/a') + '#' + i``. Parts provided by the user that exceed the maximum value size, can themselves be broken into internal parts, thus it is possible to have a second level of data segmentation, encoded in keys as ``'_' + hash('mybucket/a') + '#' + i + '.' + j``.

Each read or write operation results in one metadata get at the backend, one or more key reads/writes (depending on how many parts the object consists of and how the read/write boundary overlaps with those part offsets/lengths), and a metadata write. We avoid writing over the current boundary of object data parts, as in Kreon this requires reallocating space for the key-value pair. Instead, we write a new part to enlarge an object.

Multipart data is handled in the exact same way. Part ``i`` of data belonging to ``mybucket$b`` goes into key ``'_' + hash('mybucket$b') + '#' + i``. Any internal parts go into ``... '#' + i + '.' + j``. When a multipart object is complete, it is moved to the "standard" object namespace.

*Note: There has been a discussion on splitting up data into extents and storing the extents as write-once, content-hashed blocks. This has pros (fast copies, easy versioning, data deduplication, snapshots) and cons (hash lists in metadata management, hash calculation, garbage collection).*

Implementation outline
----------------------

The following table outlines in pseudocode how H3 operations are implemented with key-value backend functions, where:

    | ``user_id = '@' + <user_name>``
    | ``bucket_id = <bucket name>``
    | ``object_id = <bucket name> + '/' + <object_name>``
    | ``object_part_id = '_' + hash(object_id) + '#' + <part_id> + ['.' + <subpart_id>]``
    | ``multipart_id = <bucket name> + '$' + <multipart_name>``
    | ``multipart_part_id = '_' + hash(multipart_id) + '#' + <part_id> + ['.' + <subpart_id>]``

:Create bucket:
    | ``get(key=user_id)``
    | ``create(key=bucket_id, value=bucket_metadata)``
    | ``put(key=user_id)``
:Delete bucket:
    | ``get(key=user_id)``
    | ``if scan(prefix=bucket_id + '/') == empty: delete(key=bucket_id)``
    | ``put(key=user_id)``
:List buckets:
    | ``get(key=user_id)``
:Get bucket info:
    | ``get(key=bucket_id)``

:Create object:
    | ``get(key=bucket_id)``
    | ``if not exists(key=object_id): create(key=object_id, value=object_metadata)``
:Delete object:
    | ``get(key=object_id)``
    | ``for key in scan(prefix='_' + hash(object_id)): delete(key)``
    | ``delete(key=object_id)``
:Read object:
    | ``get(key=object_id)``
    | ``get(key=object_part_id, offset, length)`` (one or more)
    | ``put(key=object_id)``
:Write object:
    | ``get(key=object_id)``
    | ``put(key=object_part_id, offset, length, data)`` (one or more)
    | ``put(key=object_id)``
:Write object from object:
    | ``get(key=src_object_id)``
    | ``get(key=dest_object_id)``
    | ``put(key=dest_object_part_id, dest_offset, dest_length, get(key=src_object_part_id, src_offset, src_length))`` (one or more)
    | ``put(key=src_object_id)``
    | ``put(key=dest_object_id)``
:Copy object:
    | ``get(key=src_object_id)``
    | ``if exists(key=dest_object_id): delete_object(object_id)``
    | ``for key in scan(prefix='_' + hash(src_object_id)): copy(src_key=key, dest_key=change_prefix(key))``
    | ``create(key=dest_object_id)``
:Move object:
    | ``get(key=src_object_id)``
    | ``if exists(key=dest_object_id): delete_object(object_id)``
    | ``for key in scan(prefix='_' + hash(src_object_id)): move(src_key=key, dest_key=change_prefix(key))``
    | ``create(key=dest_object_id)``
    | ``delete(key=src_object_id)``
:List objects:
    | ``get(key=bucket_id)``
    | ``scan(prefix=bucket_id + '/')``
:Get object info:
    | ``get(key=object_id)``

:Create multipart:
    As *Create object*.
:Complete multipart:
    As *Move object*.
:Abort multipart:
    As *Delete object*.
:List parts:
    As *Get object info*.
:Write part:
    As *Write object*.
:Write part from object:
    As *Write object from object*.

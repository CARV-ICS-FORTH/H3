Design
======

H3 is a thin, stateless layer that provides object semantics on top of a high-performance key-value store - a typical service deployed in HPC and Big Data environments. By transitioning a cloud-based S3 service to H3 and running in a cluster, we expect applications to enjoy much faster data operations and - if the key-value store is distributed - to easily scale out accross all cluster nodes. In the later case, the object service is not provided centrally, but *everywhere* on the cluster.

Overview
--------

H3 provides a flat organization scheme where each data object is linked to a globally unique identifier called bucket. Buckets belong to users and contain objects. The H3 API supports typical bucket operations, such as create, list, and delete. Object management includes reading/writing objects from/to H3, copying, renaming, listing, and deleting. H3 also supports multipart operations, where objects are written in parts and coalesced at the end.

In essence, H3 implements a translation layer between the object namespace and a key-value store, similar to how a filesystem provides a hierarchical namespace of files on top of a block device. However, there are major differences:

* The object namespace is *pseudo hierarchical*, meaning there is no real hierarchy imposed. Object names can contain ``/`` as a delimiter and the list operation supports a prefix parameter to return all respective objects; like issuing a ``find <path>`` command in a filesystem, but not an ``ls <path>``.
* Buckets can only include data files, not special files, like links, sockets, etc.
* The key-value store provides a much richer set of data query and manipulation primitives, in contrast to a typical block device. It can handle arbitrary value sizes, scan keys (return all keys starting with a prefix), operate on multiple keys in a single transaction, etc. H3 takes advantage of those primitives in order to minimize code complexity and exploit any optimizations done in the key-value layer.

H3 is provided as C library, called ``h3lib``. ``h3lib`` implements the object API as a series of functions that convert the bucket and object operations to operations in the provided key-value backend. The key-value store interface is abstracted into a common API with implementations for `RocksDB <https://rocksdb.org>`_, `Redis <https://redis.io>`_, Kreon, and a filesystem.

User management
---------------

H3 maps user credentials, given at library initialization, to a user id with the help of an external service [TBD]. The user id is passed along in all H3 calls and stored in bucket/object metadata when they are created. Bucket/object operations fail if the provided user id does not match the respective id stored for the specific entity.

Note that H3 cannot enforce strict data protection semantics, due to its nature - being a library layered on top of a key-value store and not a REST service. Complete data isolation between users can really only be accomplished either by mapping H3 users to backend domains of some kind (assuming the key-value store supports it), or by using separate key-value deployments/namespaces per user.

Data organization
-----------------

Users, buckets and objects in H3 have associated metadata. Objects also have data, that - depending on the creation/write method - can consist of a single or multiple parts of variable size. We assume that the key-value backend may have size limits on either keys and/or values thus H3 will automatically break over-sized values into an appropriate number of smaller parts.

Metadata
^^^^^^^^

Each user, bucket and object in H3 has a unique name identifier corresponding to a key in the backend holding its metadata. User keys are formulated by concatenating ``'@'`` and the user id (eg. ``@42``). Bucket keys are the bucket names. Object keys are produced by concatenating the bucket name, ``/`` and the full object name (eg. ``mybucket/a``, ``mybucket/a/b/c``).

There are no limitations on the characters that can be used for object names. Bucket names cannot include ``/`` (Amazon S3 requires compliance with DNS naming).

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

We handle multipart data writes (multipart upload) with special types of objects, which exist in the namespace that results from concatenating the bucket name with marker ``$`` and an identifier generated internally.

Data
^^^^

In the simplest case, where object data is smaller than the value size limitation, we just store the data under a UUID (eg. data for ``mybucket/a`` goes into key ``'_' + <UUID>``). The UUID is a 16-byte random value, making part identifiers independent of their parent object thus enabling fast rename operations. The underscore is added at the beginning to ensure that data keys are not intermixed with bucket/object name keys during scan operations.

Object data is broken up into parts if it is larger than the value size limitation, or is provided as such by the user via the multipart API. Part ``i`` of data belonging to ``mybucket/a`` goes into key ``'_' + <UUID> + '#' + i``. Parts provided by the user that exceed the maximum value size, can themselves be broken into internal parts, thus it is possible to have a second level of data segmentation, encoded in keys as ``'_' + <UUID> + '#' + i + '.' + j``.

Each read or write operation results in one metadata get at the backend, one or more key reads/writes (depending on how many parts the object consists of and how the read/write boundary overlaps with those part offsets/lengths), and a metadata write. We avoid writing over the current boundary of object data parts, as in Kreon this requires reallocating space for the key-value pair. Instead, we write a new part to enlarge an object.

Multipart data is handled in the exact same way. Part ``i`` of data belonging to ``mybucket$b`` goes into key ``'_' + <UUID> + '#' + i``. Any internal parts go into ``'_' + <UUID> '#' + i + '.' + j``. When a multipart object is complete, it is moved to the "standard" object namespace. The UUID generated, is actually used as the multipart identifier returned to the user and the mapping from UUID to bucket and object name is stored at ``% + <UUID>``.

*Note: There has been a discussion on splitting up data into extents and storing the extents as write-once, content-hashed blocks. This has pros (fast copies, easy versioning, data deduplication, snapshots) and cons (hash lists in metadata management, hash calculation, garbage collection).*

Implementation outline
----------------------

The following table outlines in pseudocode how H3 operations are implemented with key-value backend functions, where:

    | ``user_id = '@' + <user_name>``
    | ``bucket_id = <bucket name>``
    | ``object_id = <bucket name> + '/' + <object_name>`` (for non-multipart objects)
    | ``object_id = <bucket name> + '$' + <object_name>`` (for multipart objects)
    | ``object_part_id = '_' + <UUID> + '#' + <part_number> + ['.' + <subpart_number>]``
    | ``multipart_id = '%' + <UUID>``

:Create bucket:
    | ``user_metadata = get(key=user_id)``
    | ``create(key=bucket_id, value=bucket_metadata)``
    | ``user_metadata += bucket_id``
    | ``put(key=user_id, value=user_metadata)``
:Delete bucket:
    | ``user_metadata = get(key=user_id)``
    | ``if bucket not in user_metadata.buckets: abort``
    | ``if scan(prefix=bucket_id + '/') == empty: delete(key=bucket_id), user_metadata -= bucket_id``
    | ``put(key=user_id, value=user_metadata)``
:List buckets:
    | ``user_metadata = get(key=user_id)``
    | ``produce list from user_metadata``
:Get bucket info:
    | ``bucket_metadata = get(key=bucket_id)``
    | ``if user_id != bucket_metadata.user_id: abort``
    | ``if not gather_statistics: return``
    | ``foreach object in scan(prefix=bucket_id + '/'): object_metadata = get(key_object_id)``
    | ``produce statistics from all metadata``

:Create object:
    | ``bucket_metadata = get(key=bucket_id)``
    | ``if bucket_metadata.user_id != user_id: abort``
    | ``if exists(key=object_id): abort``
    | ``create(key=object_id, value=object_metadata)``
    | If data is provided, as *Write object*.
:Copy object from object data:
    | As *Create object*, with data as *Read object*.
:Delete object:
    | ``object_metadata = get(key=object_id)``
    | ``if user_id != object_metadata.user_id: abort``
    | ``for object_part_id in object_metadata.parts: delete(object_part_id)``
    | ``if error: object_metadata.is_bad = true, abort``
    | ``delete(key=object_id)``
:Read object:
    | ``object_metadata = get(key=object_id)``
    | ``if object_metadata.is_bad: abort``
    | ``if user_id != object_metadata.user_id: abort``
    | ``get(key=object_part_id, offset, length)`` (one or more)
    | ``update object_metadata timestamps``
    | ``put(key=object_id, value=object_metadata)``
:Write object:
    | ``object_metadata = get(key=object_id)``
    | ``if user_id != object_metadata.user_id: abort``
    | ``put(key=object_part_id, offset, length, data)`` (one or more)
    | ``if error: object_metadata.is_bad = true, abort``
    | ``update object_metadata timestamps``
    | ``put(key=object_id, value=object_metadata)``
:Write object from object data:
    | As *Write object*, with data from another object as *Read object*.
:Copy object:
    | ``object_metadata = get(key=src_object_id)``
    | ``if user_id != object_metadata.user_id: abort``
    | ``if exists(key=dest_object_id) and abort_if_exists: abort``
    | ``for key in scan(prefix='_' + hash(src_object_id)): copy(src_key=key, dest_key=change_prefix(key))``
    | ``create(key=dest_object_id, value=change_metadata(object_metadata))``
:Move object:
    | ``object_metadata = get(key=src_object_id)``
    | ``if user_id != object_metadata.user_id: abort``
    | ``if exists(key=dest_object_id) and abort_if_exists: abort``
    | ``update object_metadata timestamps``
    | ``put(key=dest_object_id, value=object_metadata)``
:List objects:
    | ``bucket_metadata = get(key=bucket_id)``
    | ``if user_id != bucket_metadata.user_id: abort``
    | ``scan(prefix=bucket_id + '/')``
    | ``produce list from results``
:Get object info:
    | ``object_metadata = get(key=sobject_id)``
    | ``if user_id != object_metadata.user_id: abort``

:Create multipart:
    | As *Create object*.
    | ``put(key=multipart_id, value=multipart_metadata)``
:Complete multipart:
    | ``multipart_metadata = get(key=multipart_id)``
    | As *Move object*.
:Abort multipart:
    | ``multipart_metadata = get(key=multipart_id)``
    | As *Delete object*.
:List parts:
    | ``multipart_metadata = get(key=multipart_id)``
    | As *Get object info*.
    | ``produce list from object_metadata``
:Create part:
    | ``multipart_metadata = get(key=multipart_id)``
    | As *Write object*.
:Create part from object:
    | ``multipart_metadata = get(key=multipart_id)``
    | As *Write object from object*.

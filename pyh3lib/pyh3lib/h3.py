# Copyright [2019] [FORTH-ICS]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from . import h3lib

class H3List(list):
    """A list that has a ``done`` attribute. If ``done`` is ``False``
    there are more items to be fetched, so repeat the call
    with an appropriate offset to get the next batch.
    """
    def __new__(self, *args, **kwargs):
        return super().__new__(self, args, kwargs)

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and hasattr(args[0], '__iter__'):
            list.__init__(self, args[0])
        else:
            list.__init__(self, args)
        self.__dict__.update(kwargs)

    def __call__(self, **kwargs):
        self.__dict__.update(kwargs)
        return self

class H3Version(type):
    @property
    def VERSION(self):
        return h3lib.version()

class H3(object, metaclass=H3Version):
    """Python interface to H3.

    :param config_file: file containing configuration options for storage type
    :param storage_type: backend storage type (choose one of ``STORE_*`` class attributes)
    :param user_id: user performing all actions
    :type storage_type: int
    :type config_file: string
    :type user_id: int

    .. note::
       All functions may raise standard exceptions on internal errors, or some ``pyh3lib.*Error``
       in respect to the underlying library's return values.
    """

    BUCKET_NAME_SIZE = h3lib.H3_BUCKET_NAME_SIZE
    """Maximum bucket name size."""

    OBJECT_NAME_SIZE = h3lib.H3_OBJECT_NAME_SIZE
    """Maximum object name size."""

    STORE_CONFIG = h3lib.H3_STORE_CONFIG
    """Storage type to use whatever is set as ``store`` in the ``H3`` section of the config file (default).
    Possible values for ``store`` are: ``filesystem``, ``kreon``, ``rocksdb``."""

    STORE_FILESYSTEM = h3lib.H3_STORE_FILESYSTEM
    """Storage type to use the filesystem as the backend.
    
    The config file should contain a ``FILESYSTEM`` section, with the following variables:

    ========  =================
    ``root``  /tmp/h3 (default)
    ========  =================
    """

    STORE_KREON = h3lib.H3_STORE_KREON
    """Storage type to use Kreon as the backend."""

    STORE_ROCKSDB = h3lib.H3_STORE_ROCKSDB
    """Storage type to use `RocksDB <https://rocksdb.org>`_ as the backend."""

    # STORE_REDIS = h3lib.H3_STORE_REDIS
    # """Storage type to use Redis as the backend."""

    # STORE_IME = h3lib.H3_STORE_IME
    # """Storage type to use IME as the backend."""

    def __init__(self, config_file, storage_type=h3lib.H3_STORE_CONFIG, user_id=0):
        self._handle = h3lib.init(config_file, storage_type)
        if not self._handle:
            raise SystemError('Could not create H3 handle')
        self._user_id = user_id

    def list_buckets(self):
        """List all buckets.

        :returns: A list of bucket names if the call was successfull
        """

        return h3lib.list_buckets(self._handle, self._user_id)

    def info_bucket(self, bucket_name, get_stats=False):
        """Get bucket information.

        :param bucket_name: the bucket name
        :param get_stats: include statistics in the reply (default is no)
        :type bucket_name: string
        :type get_stats: boolean
        :returns: A named tuple with bucket information if the call was successful

        The returned tuple has the following fields:

        ============  ===============
        ``creation``  <timestamp>
        ``stats``     <tuple> or None
        ============  ===============

        If stats is populated, it has the following fields:

        =====================  ===========
        ``size``               <int>
        ``count``              <int>
        ``last_access``        <timestamp>
        ``last_modification``  <timestamp>
        =====================  ===========

        .. note::
           A request to get stats requires iterating over all objects in the bucket, which may take significant time to complete.
        """

        return h3lib.info_bucket(self._handle, bucket_name, get_stats, self._user_id)

    def create_bucket(self, bucket_name):
        """Create a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_bucket(self._handle, bucket_name, self._user_id)

    def delete_bucket(self, bucket_name):
        """Delete a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful

        .. note::
           A bucket must be empty in order to be deleted.
        """
        return h3lib.delete_bucket(self._handle, bucket_name, self._user_id)

    def list_objects(self, bucket_name, prefix='', offset=0):
        """List objects in a bucket.

        :param bucket_name: the bucket name
        :param prefix: list only objects starting with prefix (default is no prefix)
        :param offset: continue list from offset (default is to start from the beginning)
        :type bucket_name: string
        :type prefix: string
        :type offset: int
        :returns: An H3List of object names if the call was successfull
        """

        objects, done = h3lib.list_objects(self._handle, bucket_name, prefix, offset, self._user_id)
        return H3List(objects, done=done)

    def info_object(self, bucket_name, object_name):
        """Get object information.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: A named tuple with object information if the call was successful

        The returned tuple has the following fields:

        =====================  ===========
        ``is_bad``             <boolean>
        ``size``               <int>
        ``creation``           <timestamp>
        ``last_access``        <timestamp>
        ``last_modification``  <timestamp>
        =====================  ===========
        """

        return h3lib.info_object(self._handle, bucket_name, object_name, self._user_id)

    def create_object(self, bucket_name, object_name, data):
        """Create an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the contents
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_object(self._handle, bucket_name, object_name, data, self._user_id)

    def create_object_copy(self, bucket_name, src_object_name, offset, size, dst_object_name):
        """Create an object with data from another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type offset: int
        :type size: int
        :type dst_object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_object_copy(self._handle, bucket_name, src_object_name, offset, size, dst_object_name, self._user_id)

    def write_object(self, bucket_name, object_name, data, offset):
        """Write to an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the data to write
        :param offset: the offset in the object where writing should start
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :type offset: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.write_object(self._handle, bucket_name, object_name, data, offset, self._user_id)

    def write_object_copy(self, bucket_name, src_object_name, src_offset, size, dst_object_name, dst_offset):
        """Write to an object with data from another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param src_offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param dst_object_name: the destination object name
        :param dst_offset: the offset in the destination object where writing should start
        :type bucket_name: string
        :type src_object_name: string
        :type src_offset: int
        :type size: int
        :type dst_object_name: string
        :type dst_offset: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.write_object_copy(self._handle, bucket_name, src_object_name, src_offset, size, dst_object_name, dst_offset, self._user_id)

    def read_object(self, bucket_name, object_name, offset, size):
        """Read from an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param offset: the offset in the object where reading should start
        :param size: the size of the data to read
        :type bucket_name: string
        :type object_name: string
        :type offset: int
        :type size: int
        :returns: The bytes read if the call was successful
        """

        return h3lib.read_object(self._handle, bucket_name, object_name, offset, size, self._user_id)

    def copy_object(self, bucket_name, src_object_name, dst_object_name, no_overwrite=False):
        """Copy an object to another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :param no_overwrite: do not overwrite the destination if it exists (default is to overwrite)
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :type no_overwrite: boolean
        :returns: ``True`` if the call was successful
        """

        return h3lib.copy_object(self._handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

    def move_object(self, bucket_name, src_object_name, dst_object_name, no_overwrite=False):
        """Move/rename an object to another object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :param no_overwrite: do not overwrite the destination if it exists (default is to overwrite)
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :type no_overwrite: boolean
        :returns: ``True`` if the call was successful
        """

        return h3lib.move_object(self._handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

    def delete_object(self, bucket_name, object_name):
        """Delete an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.delete_object(self._handle, bucket_name, object_name, self._user_id)

    def list_multiparts(self, bucket_name, offset=0):
        """List all multipart IDs for a bucket.

        :param bucket_name: the bucket name
        :param offset: continue list from offset (default is to start from the beginning)
        :type bucket_name: string
        :type offset: int
        :returns: An H3List of multipart ids if the call was successfull
        """

        multiparts, done = h3lib.list_multiparts(self._handle, bucket_name, offset, self._user_id)
        return H3List(multiparts, done=done)

    def create_multipart(self, bucket_name, object_name):
        """Create a multipart object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: The multipart id if the call was successful
        """

        return h3lib.create_multipart(self._handle, bucket_name, object_name, self._user_id)

    def complete_multipart(self, multipart_id):
        """Complete a multipart object (creates the actual object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """
        return h3lib.complete_multipart(self._handle, multipart_id, self._user_id)

    def abort_multipart(self, multipart_id):
        """Abort a multipart object (deletes the multipart object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.abort_multipart(self._handle, multipart_id, self._user_id)

    def list_parts(self, multipart_id):
        """List all parts of a multipart object.

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: A list of named tuples, each with part information if the call was successful

        The returned list contains tuples having the following fields:

        ===============  =====
        ``part_number``  <int>
        ``size``         <int>
        ===============  =====
        """

        return h3lib.list_parts(self._handle, multipart_id, self._user_id)

    def create_part(self, multipart_id, part_number, data):
        """Create a part of a multipart object.

        :param multipart_id: the multipart id
        :param part_number: the part to create
        :param data: the contents
        :type multipart_id: string
        :type part_number: int
        :type data: bytes
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_part(self._handle, multipart_id, part_number, data, self._user_id)

    def create_part_copy(self, object_name, offset, size, multipart_id, part_number):
        """Create a part of a multipart object with data from another object.

        :param object_name: the source object name
        :param offset: the offset in the source object to start copying data from
        :param size: the size of the data to copy
        :param multipart_id: the multipart id
        :param part_number: the part to create
        :type object_name: string
        :type offset: int
        :type size: int
        :type multipart_id: string
        :type part_number: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_part_copy(self._handle, object_name, offset, size, multipart_id, part_number, self._user_id)

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
import time
import struct

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

class H3Bytes(bytes):
    """A bytes object with a ``done`` attribute. If ``done`` is ``False``
    there is more data to be read, so repeat the call
    with an appropriate offset to get the next batch.
    """
    def __new__(self, *args, **kwargs):
        obj = super().__new__(self, *args)
        obj.__dict__.update(kwargs)
        return obj

class H3Version(type):
    @property
    def VERSION(self):
        return h3lib.version()

class H3Cache(object, metaclass=H3Version):
    """Python interface to H3Cache.

    :param hot_storage_uri: hot backend storage URI
    :param cold_storage_uri: cold backend storage URI
    :param user_id: user performing all actions
    :type hot_storage_uri: string
    :type cold_storage_uri: string
    :type user_id: int

    Example backend URIs include (defaults for each type shown):

    * ``file:///tmp/h3`` for local filesystem
    * ``kreon://127.0.0.1:2181`` for Kreon, where the network location refers to the ZooKeeper host and port
    * ``rocksdb:///tmp/h3/rocksdb`` for `RocksDB <https://rocksdb.org>`_
    * ``redis://127.0.0.1:6379`` for `Redis <https://redis.io>`_

    .. note::
       All functions may raise standard exceptions on internal errors, or some ``h3lib.*Error``
       in respect to the underlying library's return values.
    """

    BUCKET_NAME_SIZE = h3lib.H3_BUCKET_NAME_SIZE
    """Maximum bucket name size."""

    OBJECT_NAME_SIZE = h3lib.H3_OBJECT_NAME_SIZE
    """Maximum object name size."""
     
    METADATA_NAME_SIZE = h3lib.H3_METADATA_NAME_SIZE
    """Maximum metadata name size."""

    def __init__(self, hot_storage_uri, cold_storage_uri, user_id=0):
        self._hot_handle = h3lib.init(hot_storage_uri)
        self._cold_handle = h3lib.init(cold_storage_uri)
        if not self._hot_handle:
            raise SystemError('Could not create H3 hot storage handle')
        if not self._cold_handle:
            raise SystemError('Could not create H3 cold storage handle')
        self._user_id = user_id

    def __register_object_expiration_time__(self, bucket_name, object_name):
        """Marks the object with the ``CachetAt`` attribute. 
        
        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: nothing
        """

        # get the time now
        now = time.clock_gettime(time.CLOCK_REALTIME)

        cached_at = struct.pack('d', now)

        # Cached at
        return h3lib.create_object_metadata(self._cold_handle, bucket_name, object_name, "CachedAt", cached_at, self._user_id)

    def __fetch_data_from_cold__(self, bucket_name, object_name):
        """Fetches an object from the cold storage to the hot
        
        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: nothing
        """

        # create the bucket if it is not exists
        try:
            h3lib.create_bucket(self._hot_handle, bucket_name) 
        # pass only the case that the bucket already exists
        except h3lib.ExistsError:
            pass

        offset = 0
        while True:
            # read all the object from the cold storage
            data, done = h3lib.read_object(self._cold_handle, bucket_name, object_name, offset, 0, self._user_id)
            
            if data is None:
                data = b''

            # write it to the hot
            h3lib.write_object(self._hot_handle, bucket_name, object_name, data, offset, self._user_id)
            
            if done:
                break
            
            offset += len(data)

        # register when the object must be expires from the hot
        self.__register_object_expiration_time__(bucket_name, object_name)

    def __write_data_to_hot__(self, write_function, bucket_name, object_name, data, offset=0):
        """Writes an object in the hot storage. 
        
        :param write_function: the write callback function
        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the objects data
        :param offset: the offset in the object where writing should start
        :type write_function: function
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :type offset: int
        :returns: ``True`` if the call was successful

        .. note::
            * In case the offset is not zero and is not exists in the hot, it must be fetched from the cold storage first.
            * In case the offset is zero the objects is written in the hot directly.
        """

        # create the bucket in the hot
        try:
            h3lib.create_bucket(self._hot_handle, bucket_name) 
        # pass only the case that the bucket already exists
        except h3lib.ExistsError:
            pass
        
        if offset:
            # check if the object exists in the hot
            try:
                if not h3lib.object_exists(self._hot_handle, bucket_name, object_name):
                    # fetch the object from the cold storage
                    self.__fetch_data_from_cold__(bucket_name, object_name)
            except h3lib.NotExistsError:
                pass
        
        if write_function(self._hot_handle, bucket_name, object_name, data, offset, self._user_id):
            if not self.__touch_object_in_cold_storage__(bucket_name, object_name):
                self.__create_pseudo_object_in_cold__(bucket_name, object_name)

                # register when the object must be expires from the hot
                return self.__register_object_expiration_time__(bucket_name, object_name)
            return True

        return False

    def __create_object_in_hot__(self, create_function, bucket_name, object_name, data):
        """Creates an object in the hot storage. 
        
        :param create_function: the create callback function
        :param bucket_name: the bucket name
        :param object_name: the object name
        :param data: the objects data
        :type create_function: function
        :type bucket_name: string
        :type object_name: string
        :type data: bytes
        :returns: ``True`` if the call was successful

        .. note::
            * In case the object exists in the cold it raises an ``ExistError`` exception.
            * Otherwise the object is created in the hot storage and a pseudo object is create in the
              cold storage.
        """

        # if the object exists in the cold storage, abort
        if h3lib.object_exists(self._cold_handle, bucket_name, object_name):
            raise h3lib.ExistsError
        
        # create the bucket if is not exists
        try:
            h3lib.create_bucket(self._hot_handle, bucket_name)
        except h3lib.ExistsError:
            pass

        if create_function(self._hot_handle, bucket_name, object_name, data, self._user_id):
            self.__create_pseudo_object_in_cold__(bucket_name, object_name) 
            
            # register when the object must be expires from the hot
            return self.__register_object_expiration_time__(bucket_name, object_name)    

        return False

    def __create_pseudo_object_in_cold__(self, bucket_name, object_name):
        """Creates a pseudo object in the cold storage. 
        
        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        # take the info
        info = h3lib.info_object(self._hot_handle, bucket_name, object_name) 

        try:
            # create a speydo object in the cold storage
            return h3lib.create_pseudo_object(self._cold_handle, bucket_name, object_name, info.is_bad, info.read_only,
                                                info.size, info.creation, info.last_access, info.last_modification, info.last_change,
                                                info.mode, info.uid, info.gid)      
        except h3lib.ExistsError:
            return True

        return False

    def __touch_object_in_cold_storage__(self, bucket_name, object_name):
        """Updates the time metadata of an object in the cold storage
        
        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: nothing
        """

        # change the last access time
        try:
            info = h3lib.info_object(self._hot_handle, bucket_name, object_name)

            if info is not None:
                h3lib.touch_object(self._cold_handle, bucket_name, object_name, info.last_access, info.last_modification, self._user_id)
        except h3lib.NotExistsError:
            return False
        
        return True

    def info_storage(self):
        """Get H3 storage info for both hot and cold

        :returns: A dictionary with both storage information if the call was successful

        The returned dictionary has the following fields for both cold and hot storage:

        ===============  ===============
        ``total_space``  <int>
        ``free_space``   <int>
        ``used_space``   <int>
        ===============  ===============
        """

        return {
            "hot" : h3lib.info_storage(self._hot_handle), 
            "cold": h3lib.info_storage(self._cold_handle)
        }

    def list_buckets(self):
        """List all buckets in the hot & cold storage.

        :returns: A list of bucket names if the call was successful
        """

        return h3lib.list_buckets(self._cold_handle, self._user_id)

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

        return h3lib.info_bucket(self._cold_handle, bucket_name, get_stats, self._user_id)

    def create_bucket(self, bucket_name):
        """Create a bucket in the cold storage.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_bucket(self._cold_handle, bucket_name, self._user_id)

    def delete_bucket(self, bucket_name):
        """Delete a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful

        .. note::
           A bucket must be empty in order to be deleted.
        """

        # Check if the bucket is in the hot in order to delete it.
        try:
            h3lib.delete_bucket(self._hot_handle, bucket_name)
        # The bucket does not exists in the hot
        except h3lib.NotExistsError:
            pass

        return h3lib.delete_bucket(self._cold_handle, bucket_name, self._user_id)

    def purge_bucket(self, bucket_name):
        """Purge a bucket.

        :param bucket_name: the bucket name
        :type bucket_name: string
        :returns: ``True`` if the call was successful
        """

        # Check if we have the bucket in the hot in order to delete it.
        try:
            h3lib.purge_bucket(self._hot_handle, bucket_name)
        # The bucket does not exists in the hot
        except h3lib.NotExistsError:
            pass

        return h3lib.purge_bucket(self._cold_handle, bucket_name, self._user_id)

    def list_objects(self, bucket_name, prefix='', offset=0, count=10000):
        """List objects in a bucket.

        :param bucket_name: the bucket name
        :param prefix: list only objects starting with prefix (default is no prefix)
        :param offset: continue list from offset (default is to start from the beginning)
        :param count: number of object names to retrieve
        :type bucket_name: string
        :type prefix: string
        :type offset: int
        :type count: int
        :returns: An H3List of object names if the call was successful
        """

        objects, done = h3lib.list_objects(self._cold_handle, bucket_name, prefix, offset, count, self._user_id)
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
        ``last_change``        <timestamp>
        =====================  ===========
        """
        
        return h3lib.info_object(self._cold_handle, bucket_name, object_name, self._user_id)

    def touch_object(self, bucket_name, object_name, last_access=-1, last_modification=-1):
        """Set object access and modification times (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param last_access: last access timestamp
        :param last_modification: last modification timestamp
        :type bucket_name: string
        :type object_name: string
        :type last_access: float
        :type last_modification: float
        :returns: ``True`` if the call was successful
        """

        # touch the object in the hot
        try:
            h3lib.touch_object(self._hot_handle, bucket_name, object_name, last_access, last_modification, self._user_id)
        except h3lib.NotExistsError:
            pass

        return h3lib.touch_object(self._cold_handle, bucket_name, object_name, last_access, last_modification, self._user_id)

    def set_object_permissions(self, bucket_name, object_name, mode):
        """Set object permissions attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param mode: permissions mode
        :type bucket_name: string
        :type object_name: string
        :type mode: int
        :returns: ``True`` if the call was successful
        """

        # set objects permissions in the hot
        try:
            h3lib.set_object_permissions(self._hot_handle, bucket_name, object_name, mode, self._user_id)
        except h3lib.NotExistsError:
            pass

        return h3lib.set_object_permissions(self._cold_handle, bucket_name, object_name, mode, self._user_id)
    
    def make_object_read_only(self, bucket_name, object_name):
        """Set object permissions attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        # make object read only in the hot
        try:
            h3lib.make_object_read_only(self._hot_handle, bucket_name, object_name, self._user_id)
        except h3lib.NotExistsError:
            pass

        return h3lib.make_object_read_only(self._cold_handle, bucket_name, object_name, self._user_id)

    def set_object_owner(self, bucket_name, object_name, uid, gid):
        """Set object owner attribute (used by h3fuse).

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param uid: user id
        :param gid: group id
        :type bucket_name: string
        :type object_name: string
        :type uid: int
        :type gid: int
        :returns: ``True`` if the call was successful
        """

        # set objects owner in the hot
        try:
            h3lib.set_object_owner(self._cold_handle, bucket_name, object_name, uid, gid, self._user_id)
        except h3lib.NotExistsError:
            pass

        return h3lib.set_object_owner(self._cold_handle, bucket_name, object_name, uid, gid, self._user_id)

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

        return self.__create_object_in_hot__(h3lib.create_object, bucket_name, object_name, data)
        
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
        :returns: The bytes copied if the call was successful
        """
        
        # if the object exists in the hot make a copie in the hot
        if h3lib.object_exists(self._hot_handle, bucket_name, src_object_name):
            # if the destination object exists in the cold storage, abort
            if h3lib.object_exists(self._cold_handle, bucket_name, dst_object_name):
                raise h3lib.ExistsError
            
            # everything is fine create the copy object in the hot
            copied_bytes = h3lib.create_object_copy(self._hot_handle, bucket_name, src_object_name, offset, size, dst_object_name, self._user_id)
            
            # register when the object must be expires from the hot
            self.__register_object_expiration_time__(bucket_name, dst_object_name)

            # update the src_object info
            self.__touch_object_in_cold_storage__(bucket_name, src_object_name)

            # take the info
            info = h3lib.info_object(self._hot_handle, bucket_name, dst_object_name) 

            # create a speydo object in the cold storage
            h3lib.create_pseudo_object(self._cold_handle, bucket_name, dst_object_name, info.is_bad, info.read_only,
                                    info.size, info.creation, info.last_access, info.last_modification, info.last_change,
                                    info.mode, info.uid, info.gid)
       
            return copied_bytes

        # else make the copy in the cold storage
        return h3lib.create_object_copy(self._cold_handle, bucket_name, src_object_name, offset, size, dst_object_name, self._user_id)
        
    def create_object_from_file(self, bucket_name, object_name, filename):
        """Create an object with data from a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :returns: ``True`` if the call was successful
        """

        return self.__create_object_in_hot__(h3lib.create_object_from_file, bucket_name, object_name, filename)

    def write_object(self, bucket_name, object_name, data, offset=0):
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

        return self.__write_data_to_hot__(h3lib.write_object, bucket_name, object_name, data, offset)

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
        :returns: The bytes copied if the call was successful
        """

        if not h3lib.object_exists(self._hot_handle, bucket_name, src_object_name):
            # fetch the object from the cold storage
            self.__fetch_data_from_cold__(bucket_name, src_object_name) 

        if not h3lib.object_exists(self._hot_handle, bucket_name, dst_object_name):
            # fetch the object from the cold storage
            self.__fetch_data_from_cold__(bucket_name, dst_object_name) 
            
        copied_bytes = h3lib.write_object_copy(self._hot_handle, bucket_name, src_object_name, src_offset, size, dst_object_name, dst_offset, self._user_id)
        
        # update the src_object info
        self.__touch_object_in_cold_storage__(bucket_name, src_object_name)
        # update the dst_object info
        self.__touch_object_in_cold_storage__(bucket_name, dst_object_name)

        return copied_bytes

    def write_object_from_file(self, bucket_name, object_name, filename, offset=0):
        """Write to an object with data from a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :param offset: the offset in the object where writing should start
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :type offset: int
        :returns: ``True`` if the call was successful
        """

        return self.__write_data_to_hot__(h3lib.write_object_from_file, bucket_name, object_name, filename, offset)

    def read_object(self, bucket_name, object_name, offset=0, size=0):
        """Read from an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param offset: the offset in the object where reading should start
        :param size: the size of the data to read (default is all)
        :type bucket_name: string
        :type object_name: string
        :type offset: int
        :type size: int
        :returns: An H3Bytes object if the call was successful
        """

        done = False
        data = None

        # try to fetch it from the hot
        try:
            data, done = h3lib.read_object(self._hot_handle, bucket_name, object_name, offset, size, self._user_id)
            
            self.__touch_object_in_cold_storage__(bucket_name, object_name)
        
        # fetch it from the cold to the hot 
        except h3lib.NotExistsError:
            # bring the data form the cold to the hot 
            self.__fetch_data_from_cold__(bucket_name, object_name)
            
            data, done = h3lib.read_object(self._hot_handle, bucket_name, object_name, offset, size, self._user_id)

            self.__touch_object_in_cold_storage__(bucket_name, object_name)

        if data is None:
            data = b''

        return H3Bytes(data, done=done)

    def read_object_to_file(self, bucket_name, object_name, filename, offset=0, size=0):
        """Read from an object into a file.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param filename: the filename
        :param offset: the offset in the object where reading should start
        :param size: the size of the data to read (default is all)
        :type bucket_name: string
        :type object_name: string
        :type filename: string
        :type offset: int
        :type size: int
        :returns: An H3Bytes object if the call was successful (empty data)
        """

        done = False

        # try to fetch it from the hot
        try:
            _, done = h3lib.read_object_to_file(self._hot_handle, bucket_name, object_name, filename, offset, size, self._user_id)
        
            self.__touch_object_in_cold_storage__(bucket_name, object_name)

        # fetch it from the cold to the hot 
        except h3lib.NotExistsError:            
            # bring the data form the cold to the hot 
            self.__fetch_data_from_cold__(bucket_name, object_name)

            # then read from the object from the hot
            _, done = h3lib.read_object_to_file(self._hot_handle, bucket_name, object_name, filename, offset, size, self._user_id)

            self.__touch_object_in_cold_storage__(bucket_name, object_name)

        return H3Bytes(done=done)

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

        status = None

        # if the object exists in the hot, copy it in the hot
        if h3lib.object_exists(self._hot_handle, bucket_name, src_object_name):

            if h3lib.object_exists(self._cold_handle, bucket_name, dst_object_name):
                if not no_overwrite:
                    status = h3lib.copy_object(self._hot_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

                    # update the dst_object info in the cold storage
                    self.__touch_object_in_cold_storage__(bucket_name, dst_object_name)
                else:
                    raise h3lib.FailureError
            else:
                status = h3lib.copy_object(self._hot_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

                # take the info
                info = h3lib.info_object(self._hot_handle, bucket_name, dst_object_name) 

                # create a speydo object in the cold storage
                h3lib.create_pseudo_object(self._cold_handle, bucket_name, dst_object_name, info.is_bad, info.read_only,
                                           info.size, info.creation, info.last_access, info.last_modification, info.last_change,
                                           info.mode, info.uid, info.gid)

            if status:
                # register when the object must be expires from the hot
                self.__register_object_expiration_time__(bucket_name, dst_object_name)

                # copy the src object metadata in the cold storage
                h3lib.copy_object_metadata(self._cold_handle, bucket_name, src_object_name, dst_object_name)
                
                # update the src_object info in the cold storage
                self.__touch_object_in_cold_storage__(bucket_name, src_object_name)
                
            return status

        # otherwise copy the object in the cold storage  
        status = h3lib.copy_object(self._cold_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)
       
        # maybe we have only the destination object in hot
        # delete it because we are going to overwrite it in the cold storage
        try:
            if not no_overwrite:
                h3lib.delete_object(self._hot_handle, bucket_name, dst_object_name, self._user_id)
        except h3lib.NotExistsError:
            pass

        return status

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

        status = None

        # if the object exists in the hot, copy it in the hot
        if h3lib.object_exists(self._hot_handle, bucket_name, src_object_name):
            
            if h3lib.object_exists(self._cold_handle, bucket_name, dst_object_name):
                if not no_overwrite:
                    h3lib.move_object_metadata(self._cold_handle, bucket_name, src_object_name, dst_object_name, self._user_id)

                    status = h3lib.move_object(self._hot_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

                    # update the dst_object info in the cold storage
                    self.__touch_object_in_cold_storage__(bucket_name, dst_object_name)
                else:
                    raise h3lib.ExistsError
            else:
                status = h3lib.move_object(self._hot_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)

                #delete all the destinations object's metadata from the cold storage

                # take the info
                info = h3lib.info_object(self._hot_handle, bucket_name, dst_object_name) 

                # create a speydo object in the cold storage
                h3lib.create_pseudo_object(self._cold_handle, bucket_name, dst_object_name, info.is_bad, info.read_only,
                                           info.size, info.creation, info.last_access, info.last_modification, info.last_change,
                                           info.mode, info.uid, info.gid)
                
                h3lib.move_object_metadata(self._cold_handle, bucket_name, src_object_name, dst_object_name, self._user_id)

            if status:
                # register when the object must be expires from the hot
                self.__register_object_expiration_time__(bucket_name, dst_object_name)
                
                # delete the object from the cold storage
                try:
                    h3lib.delete_object(self._cold_handle, bucket_name, src_object_name, self._user_id)
                except h3lib.NotExistsError:
                    pass
                
            return status

        # otherwise do it in the cold storage
        status = h3lib.move_object(self._cold_handle, bucket_name, src_object_name, dst_object_name, no_overwrite, self._user_id)
        
        # maybe we have only the destination object in hot
        # delete it because we are going to overwrite it in the cold storage
        try:
            if not no_overwrite:
                h3lib.delete_object(self._hot_handle, bucket_name, dst_object_name, self._user_id)
        except h3lib.NotExistsError:
            pass

        return status

    def exchange_object(self, bucket_name, src_object_name, dst_object_name):
        """Exchange data between objects.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :returns: ``True`` if the call was successful
        """
        
        raise NotImplementedError()

    def truncate_object(self, bucket_name, object_name, size=0):
        """Read from an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param size: the new size of the object (default is 0)
        :type bucket_name: string
        :type object_name: string
        :type size: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.truncate_object(self._cold_handle, bucket_name, object_name, size, self._user_id)

    def delete_object(self, bucket_name, object_name):
        """Delete an object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: ``True`` if the call was successful
        """

        # check if the object must be deleted from the hot too
        try:
            h3lib.delete_object(self._hot_handle, bucket_name, object_name, self._user_id)
        except h3lib.NotExistsError:
            pass

        return h3lib.delete_object(self._cold_handle, bucket_name, object_name, self._user_id)
    
    def create_object_metadata(self, bucket_name, object_name, metadata_name, metadata_value):
        """Create an object's specific metadata.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :param metadata_value: the object's metadata value
        :param size: the size of the object's metadata value
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :type metadata_value: bytes
        :type size: int
        :returns: ``True`` if the call was successful
        """

        return h3lib.create_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, metadata_value, self._user_id)
    
    def read_object_metadata(self, bucket_name, object_name, metadata_name):
        """Read an object's specific metadata.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :returns: An H3Bytes object if the call was successful
        """

        data, done = h3lib.read_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, self._user_id)
        if data is None:
            data = b''
        return H3Bytes(data, done=done)

    def delete_object_metadata(self, bucket_name, object_name, metadata_name):
        """Delete an object's specific metadata.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :param metadata_name: the object's metadata name
        :type bucket_name: string
        :type object_name: string
        :type metadata_name: string
        :returns: 
        """

        return h3lib.delete_object_metadata(self._cold_handle, bucket_name, object_name, metadata_name, self._user_id)
    
    def copy_object_metadata(self, bucket_name, src_object_name, dst_object_name):
        """Copy all the source object's metadata to the destination object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.copy_object_metadata(self._cold_handle, bucket_name, src_object_name, dst_object_name, self._user_id)
    
    def move_object_metadata(self, bucket_name, src_object_name, dst_object_name):
        """Move all the source object's metadata to the destination object.

        :param bucket_name: the bucket name
        :param src_object_name: the source object name
        :param dst_object_name: the destination object name
        :type bucket_name: string
        :type src_object_name: string
        :type dst_object_name: string
        :returns: ``True`` if the call was successful

        .. note::
            All the source object's metadata will be deleted after the move action.
            All the destination object's metadata will be deleted before the move action
        """

        return h3lib.move_object_metadata(self._cold_handle, bucket_name, src_object_name, dst_object_name, self._user_id)
    
    def list_objects_with_metadata(self, bucket_name, metadata_name, offset=0):
        """List all the objects with a specific metadata.

        :param bucket_name: the bucket name
        :param metadata_name: metadata name
        :param offset: continue list from offset (default is to start from the beginning)
        :type bucket_name: string
        :type metadata_name: string
        :type offset: int
        :returns: An H3List of object names if the call was successful
        """

        objects = h3lib.list_objects_with_metadata(self._cold_handle, bucket_name, metadata_name, offset, self._user_id)
        return H3List(objects["objects"], done=objects["done"], nextOffset=objects["nextOffset"])

    def list_multiparts(self, bucket_name, offset=0, count=10000):
        """List all multipart IDs for a bucket.

        :param bucket_name: the bucket name
        :param offset: continue list from offset (default is to start from the beginning)
        :param count: number of multipart ids to retrieve
        :type bucket_name: string
        :type offset: int
        :type count: int
        :returns: An H3List of multipart ids if the call was successful
        """

        multiparts, done = h3lib.list_multiparts(self._cold_handle, bucket_name, offset, count, self._user_id)
        return H3List(multiparts, done=done)

    def create_multipart(self, bucket_name, object_name):
        """Create a multipart object.

        :param bucket_name: the bucket name
        :param object_name: the object name
        :type bucket_name: string
        :type object_name: string
        :returns: The multipart id if the call was successful
        """

        return h3lib.create_multipart(self._cold_handle, bucket_name, object_name, self._user_id)

    def complete_multipart(self, multipart_id):
        """Complete a multipart object (creates the actual object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.complete_multipart(self._cold_handle, multipart_id, self._user_id)

    def abort_multipart(self, multipart_id):
        """Abort a multipart object (deletes the multipart object).

        :param multipart_id: the multipart id
        :type multipart_id: string
        :returns: ``True`` if the call was successful
        """

        return h3lib.abort_multipart(self._cold_handle, multipart_id, self._user_id)

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

        return h3lib.list_parts(self._cold_handle, multipart_id, self._user_id)

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

        return h3lib.create_part(self._cold_handle, multipart_id, part_number, data, self._user_id)

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

        return h3lib.create_part_copy(self._cold_handle, object_name, offset, size, multipart_id, part_number, self._user_id)

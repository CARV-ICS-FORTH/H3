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

import pytest
import pyh3lib
import struct

def test_metadata_arguments(h3):
    """Test metadata exceptions"""

    assert h3.list_buckets() == []

    assert h3.create_bucket('b1')

    h3.create_object('b1', 'o1', b'')

    assert h3.create_object_metadata('b1', 'o1', 'ExpireAt', b'')
    
    # the metadata name is not string
    with pytest.raises(TypeError):
        h3.create_object_metadata('b1', 'o1', None , b'')

    # big metadata name
    with pytest.raises(pyh3lib.H3NameTooLongError):
        h3.create_object_metadata('b1', 'o1', 'm' * (h3.METADATA_NAME_SIZE + 3) , b'')    

    # the metadata name is empty
    with pytest.raises(pyh3lib.H3InvalidArgsError):
        h3.create_object_metadata('b1', 'o1', '' , b'')
        
    # the metadata name starts with '#'
    with pytest.raises(pyh3lib.H3InvalidArgsError):
        h3.create_object_metadata('b1', 'o1', '#ExpireAt', b'')

    assert h3.purge_bucket('b1')

    assert h3.delete_bucket('b1')

def test_metadata_error(h3):
    """Test metadata exceptions"""

    assert h3.list_buckets() == []

    assert h3.create_bucket('b1')

    h3.create_object('b1', 'o1', b'')

    assert h3.create_object_metadata('b1', 'o1', 'ExpireAt', b'')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.create_object_metadata('b1', 'o2', 'ExpireAt', b'')
    
    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.delete_object_metadata('b1', 'o1', 'ReadOnlyAfter')
    
    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.read_object_metadata('b1', 'o1', 'ReadOnlyAfter')
    
    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.move_object_metadata('b1', 'o1', 'o2')
    
    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.copy_object_metadata('b1', 'o1', 'o2')

    assert h3.purge_bucket('b1')

    assert h3.delete_bucket('b1')

def test_metadata(h3):
    """Create, search and delete metadata to an empty object."""
    
    assert h3.list_buckets() == []

    assert h3.create_bucket('b1')

    h3.create_object('b1', 'o1', b'')

    h3.create_object('b1', 'o2', b'')

    h3.create_object('b1', 'o3', b'')

    assert h3.create_object_metadata('b1', 'o1', 'ExpireAt', b'')
    assert h3.create_object_metadata('b1', 'o2', 'ExpireAt', b'')
    assert h3.create_object_metadata('b1', 'o3', 'ExpireAt', b'')

    assert h3.create_object_metadata('b1', 'o3', 'Content-Type', 'XML'.encode('utf-8'))

    assert h3.read_object_metadata('b1', 'o1', 'ExpireAt') == b''

    assert h3.read_object_metadata('b1', 'o3', 'Content-Type').decode('utf-8') == 'XML'

    assert h3.create_object_metadata('b1', 'o1', 'ReadOnlyAfter', struct.pack('i', 258))

    assert h3.create_object_metadata('b1', 'o1', 'ExpireAt', struct.pack('i', 259))

    assert struct.unpack('i', h3.read_object_metadata('b1', 'o1', 'ReadOnlyAfter'))[0] == 258

    assert struct.unpack('i', h3.read_object_metadata('b1', 'o1', 'ExpireAt'))[0] == 259

    assert set(h3.list_objects_with_metadata('b1', 'ExpireAt')) == set(['o1','o2','o3'])

    assert h3.copy_object('b1', 'o1', 'o2')

    assert struct.unpack('i', h3.read_object_metadata('b1', 'o2', 'ExpireAt'))[0] == 259

    assert set(h3.list_objects_with_metadata('b1', 'ExpireAt')) == set(['o1','o2','o3'])

    assert h3.delete_object_metadata('b1', 'o1', 'ExpireAt')
    
    assert set(h3.list_objects_with_metadata('b1', 'ExpireAt')) == set(['o2','o3'])
    
    assert h3.move_object('b1', 'o1', 'o2')

    assert h3.move_object('b1', 'o2', 'o4')

    assert h3.truncate_object('b1', 'o4')

    object_info = h3.info_object('b1', 'o4')
    assert object_info.size == 0

    assert h3.list_objects_with_metadata('b1', 'ReadOnlyAfter') == ['o4']
    assert h3.list_objects_with_metadata('b1', 'ExpireAt') == ['o3']

    assert h3.delete_object('b1', 'o4')

    assert h3.list_objects_with_metadata('b1', 'ReadOnlyAfter') == []
    
    assert h3.purge_bucket('b1')
    
    assert h3.list_objects_with_metadata('b1', 'ExpireAt') == []

    assert h3.delete_bucket('b1')

def test_list_metadata(h3):
    """List object metadata."""

    assert h3.list_buckets() == []

    assert h3.create_bucket('b1')

    for i in range(1, 1500):
        h3.create_object('b1', f'object_with_very_very_large_name_to_test_metadata_{i}', b'')
        h3.create_object_metadata('b1', f'object_with_very_very_large_name_to_test_metadata_{i}', 'metadata', b'')
        
    done   = False
    offset = 0
    totalObjects = 0

    while not done:
        objects = h3.list_objects_with_metadata('b1', 'metadata', offset) 
        offset  = objects.nextOffset
        done    = objects.done

        totalObjects += len(objects)
        
    assert totalObjects == 1499

    h3.purge_bucket('b1')

    assert h3.delete_bucket('b1')
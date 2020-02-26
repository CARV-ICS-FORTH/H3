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
import random

MEGABYTE = 1048576

def test_simple(h3):
    """Create, delete an object."""

    # All empty.
    assert h3.list_buckets() == []

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.info_object('b1', 'o1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.delete_object('b1', 'o1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.read_object('b1', 'o1')

    # Create a bucket.
    assert h3.create_bucket('b1') == True

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.info_object('b1', 'o1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.delete_object('b1', 'o1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.read_object('b1', 'o1')

    assert h3.list_objects('b1') == []

    # Write the first object.
    with open('/dev/urandom', 'rb') as f:
        data = f.read(3 * MEGABYTE) 

    h3.create_object('b1', 'o1', data)

    with pytest.raises(pyh3lib.H3ExistsError):
        h3.create_object('b1', 'o1', data)

    object_info = h3.info_object('b1', 'o1')
    assert not object_info.is_bad
    assert object_info.size == (3 * MEGABYTE)
    assert type(object_info.creation) == int
    assert type(object_info.last_access) == int
    assert type(object_info.last_modification) == int

    object_data = h3.read_object('b1', 'o1')
    assert object_data == data

    object_data = h3.read_object('b1', 'o1', offset=0, size=MEGABYTE)
    assert object_data == data[:MEGABYTE]

    object_data = h3.read_object('b1', 'o1', offset=MEGABYTE, size=MEGABYTE)
    assert object_data == data[MEGABYTE:(2 * MEGABYTE)]

    object_data = h3.read_object('b1', 'o1', offset=(2 * MEGABYTE), size=MEGABYTE)
    assert object_data == data[(2 * MEGABYTE):]

    assert h3.list_objects('b1') == ['o1']

    # Write a second object.
    h3.write_object('b1', 'o2', data)

    object_info = h3.info_object('b1', 'o2')
    assert not object_info.is_bad
    assert object_info.size == (3 * MEGABYTE)
    assert type(object_info.creation) == int
    assert type(object_info.last_access) == int
    assert type(object_info.last_modification) == int

    object_data = h3.read_object('b1', 'o2')
    assert object_data == data

    assert h3.list_objects('b1') == ['o1', 'o2']

    # Overwrite second object.
    h3.write_object('b1', 'o2', data)

    object_info = h3.info_object('b1', 'o2')
    assert not object_info.is_bad
    assert object_info.size == (3 * MEGABYTE)
    assert type(object_info.creation) == int
    assert type(object_info.last_access) == int
    assert type(object_info.last_modification) == int

    object_data = h3.read_object('b1', 'o2')
    assert object_data == data

    assert h3.list_objects('b1') == ['o1', 'o2']

    # Partial overwrite second object.
    h3.write_object('b1', 'o2', data[:MEGABYTE], offset=MEGABYTE)

    object_info = h3.info_object('b1', 'o2')
    assert not object_info.is_bad
    assert object_info.size == (3 * MEGABYTE)
    assert type(object_info.creation) == int
    assert type(object_info.last_access) == int
    assert type(object_info.last_modification) == int

    object_data = h3.read_object('b1', 'o2', offset=0, size=MEGABYTE)
    assert object_data == data[:MEGABYTE]

    object_data = h3.read_object('b1', 'o2', offset=MEGABYTE, size=MEGABYTE)
    assert object_data == data[:MEGABYTE]

    object_data = h3.read_object('b1', 'o1', offset=(2 * MEGABYTE), size=MEGABYTE)
    assert object_data == data[(2 * MEGABYTE):]

    assert h3.list_objects('b1') == ['o1', 'o2']

    # Check bucket statistics.
    bucket_info = h3.info_bucket('b1', get_stats=True)
    assert bucket_info.stats != None
    assert bucket_info.stats.size == (6 * MEGABYTE)
    assert bucket_info.stats.count == 2

    # Delete first object.
    h3.delete_object('b1', 'o1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.delete_object('b1', 'o1')

    assert h3.list_objects('b1') == ['o2']

    # Move second object to third.
    h3.write_object('b1', 'o3', data)
    with pytest.raises(pyh3lib.H3FailureError):
        h3.move_object('b1', 'o2', 'o3', no_overwrite=True)

    assert 'o3' in h3.list_objects('b1')
    
    h3.move_object('b1', 'o2', 'o3')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.move_object('b1', 'o2', 'o3')

    # Copy third object back to second.
    h3.copy_object('b1', 'o3', 'o2')

    h3.copy_object('b1', 'o3', 'o2')

    with pytest.raises(pyh3lib.H3FailureError):
        h3.copy_object('b1', 'o3', 'o2', no_overwrite=True)

    object_info = h3.info_object('b1', 'o2')
    assert not object_info.is_bad
    assert object_info.size == (3 * MEGABYTE)
    assert type(object_info.creation) == int
    assert type(object_info.last_access) == int
    assert type(object_info.last_modification) == int

    object_data = h3.read_object('b1', 'o2', 0, MEGABYTE)
    assert object_data == data[:MEGABYTE]

    object_data = h3.read_object('b1', 'o2', MEGABYTE, MEGABYTE)
    assert object_data == data[:MEGABYTE]

    object_data = h3.read_object('b1', 'o2', (2 * MEGABYTE), MEGABYTE)
    assert object_data == data[(2 * MEGABYTE):]

    assert h3.list_objects('b1') == ['o2', 'o3']

    h3.delete_object('b1', 'o3')

    assert h3.list_objects('b1') == ['o2']

    with pytest.raises(pyh3lib.H3NotEmptyError):
        h3.delete_bucket('b1')

    # Delete second object.
    h3.delete_object('b1', 'o2')

    assert h3.list_objects('b1') == []

    assert h3.delete_bucket('b1') == True

    assert h3.list_buckets() == []

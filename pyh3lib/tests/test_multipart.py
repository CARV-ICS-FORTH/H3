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

    with pytest.raises(pyh3lib.H3FailureError):
        h3.create_multipart('b1', 'm1')

    # Create a bucket.
    assert h3.create_bucket('b1') == True

    assert h3.list_multiparts('b1') == []

    # Create a multipart object.
    multipart = h3.create_multipart('b1', 'm1')

    assert h3.list_multiparts('b1') == ['m1']

    with open('/dev/urandom', 'rb') as f:
        data = f.read(3 * MEGABYTE)

    h3.create_object('b1', 'o1', data)

    h3.create_part(multipart, 1, data)

    h3.create_part(multipart, 0, data[:MEGABYTE])

    h3.create_part(multipart, 2, data)

    h3.create_part_copy('o1', 0, MEGABYTE, multipart, 0)

    parts = h3.list_parts(multipart)
    assert len(parts) == 3
    for part_number, size in parts:
        if part_number == 0:
            assert size == MEGABYTE
        else:
            assert size == (3 * MEGABYTE)

    h3.complete_multipart(multipart)

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.complete_multipart(multipart)

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.abort_multipart(multipart)

    assert 'm1' in h3.list_objects('b1')

    object_info = h3.info_object('b1', 'm1')
    assert not object_info.is_bad
    assert object_info.size == (7 * MEGABYTE)
    assert type(object_info.creation) == float
    assert type(object_info.last_access) == float
    assert type(object_info.last_modification) == float
    assert type(object_info.last_change) == float

    # Delete objects.
    h3.delete_object('b1', 'm1')

    h3.delete_object('b1', 'o1')

    assert h3.list_objects('b1') == []

    assert h3.delete_bucket('b1') == True

    assert h3.list_buckets() == []

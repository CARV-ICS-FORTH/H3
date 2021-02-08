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

def test_simple(h3):
    """List, create, delete a bucket."""

    assert h3.list_buckets() == []

    assert h3.create_bucket('b1') == True

    with pytest.raises(pyh3lib.H3ExistsError):
        h3.create_bucket('b1')

    bucket_info = h3.info_bucket('b1')
    assert bucket_info.stats == None
    assert type(bucket_info.creation) == float

    bucket_info = h3.info_bucket('b1', get_stats=True)
    assert bucket_info.stats != None
    assert bucket_info.stats.size == 0
    assert bucket_info.stats.count == 0

    assert h3.list_buckets() == ['b1']

    assert h3.delete_bucket('b1') == True

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.delete_bucket('b1')

    with pytest.raises(pyh3lib.H3NotExistsError):
        h3.info_bucket('b1')

    assert h3.list_buckets() == []

def test_arguments(h3):
    """Pass invalid arguments."""

    # Empty name
    with pytest.raises(pyh3lib.H3InvalidArgsError):
        h3.create_bucket('')

    with pytest.raises(TypeError):
        h3.create_bucket(None)

    # Large name
    with pytest.raises(pyh3lib.H3NameTooLongError):
        h3.create_bucket('a' * (h3.BUCKET_NAME_SIZE + 1))

    # Invalid name
    with pytest.raises(pyh3lib.H3InvalidArgsError):
        h3.create_bucket('/bucketId')

    h3.create_bucket('\bucketId')
    h3.delete_bucket('\bucketId')

def test_many(h3):
    """Manage many buckets."""

    count = 100 # More than 10

    assert h3.list_buckets() == []

    for i in range(count):
        assert h3.create_bucket('bucket%d' % i) == True

    for i in random.sample(range(count), 10):
        with pytest.raises(pyh3lib.H3ExistsError):
            h3.create_bucket('bucket%d' % i)

    for i in range(count):
        bucket_info = h3.info_bucket('bucket%d' % i)
        assert bucket_info.stats == None
        assert type(bucket_info.creation) == float

    assert h3.list_buckets() == [('bucket%d' % i) for i in range(count)]

    for i in range(count):
        assert h3.delete_bucket('bucket%d' % i) == True

    for i in random.sample(range(count), 10):
        with pytest.raises(pyh3lib.H3NotExistsError):
            h3.delete_bucket('bucket%d' % i)

    for i in random.sample(range(count), 10):
        with pytest.raises(pyh3lib.H3NotExistsError):
            h3.info_bucket('bucket%d' % i)

    assert h3.list_buckets() == []

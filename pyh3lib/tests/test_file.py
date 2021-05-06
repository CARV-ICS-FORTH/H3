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
import os
import hashlib

MEGABYTE = 1048576

def test_copy_folder(h3):
    """Copy in a folder."""

    cwd = os.getcwd()
    print('Working in directory: %s' % cwd)
    files = []
    for (dirpath, dirnames, filenames) in os.walk(cwd):
        for filename in filenames:
            name = os.path.join(dirpath, filename)[len(cwd):].lstrip('/')
            with open(name, 'rb') as fp:
                md5 = hashlib.md5(fp.read()).hexdigest()
            files.append({'name': name,
                          'size': os.path.getsize(name),
                          'md5': md5})

    # Create bucket
    bucket_name = 'test-copy'
    assert bucket_name not in h3.list_buckets()
    assert h3.create_bucket(bucket_name) == True

    # Copy in all files in current directory and verify sizes
    for f in files:
        print('Copying: %s -> h3://%s/%s (%s bytes)' % (f['name'], bucket_name, f['name'], f['size']))
        assert h3.write_object_from_file(bucket_name, f['name'], f['name']) == True
        info = h3.info_object(bucket_name, f['name'])
        assert info.is_bad == False
        assert info.size == f['size']

    # Get the list of objects
    objects = []
    while True:
        result = h3.list_objects(bucket_name, offset=len(objects))
        objects += result
        if result.done:
            break

    assert len(objects) == len(files)

    # Verify sizes again
    for f in files:
        print('Verifying size: h3://%s/%s' % (bucket_name, f['name']))
        info = h3.info_object(bucket_name, f['name'])
        assert info.is_bad == False
        assert info.size == f['size']

    # Verify data and rewrite
    copy_folder = 'copy'
    for f in files:
        print('Verifying: h3://%s/%s' % (bucket_name, f['name']))

        data = b''
        while True:
            result = h3.read_object(bucket_name, f['name'], offset=len(data))
            print('Got %d bytes' % len(result))
            data += result
            if result.done:
                break

        md5 = hashlib.md5(data).hexdigest()
        assert md5 == f['md5']

        print('Writing: h3://%s/%s/%s' % (bucket_name, copy_folder, f['name']))
        h3.write_object(bucket_name, copy_folder + '/' + f['name'], data)

    # Verify copy
    for f in files:
        print('Verifying: h3://%s/%s/%s' % (bucket_name, copy_folder, f['name']))
        info = h3.info_object(bucket_name, copy_folder + '/' + f['name'])
        assert info.is_bad == False
        assert info.size == f['size']

        data = b''
        while True:
            result = h3.read_object(bucket_name, copy_folder + '/' + f['name'], offset=len(data))
            print('Got %d bytes' % len(result))
            data += result
            if result.done:
                break

        md5 = hashlib.md5(data).hexdigest()
        assert md5 == f['md5']

    # Empty and delete bucket
    assert h3.purge_bucket(bucket_name) == True
    assert h3.delete_bucket(bucket_name) == True

def test_large_file(h3):
    """Copy in a folder."""

    # Create bucket
    bucket_name = 'test-large'
    assert bucket_name not in h3.list_buckets()
    assert h3.create_bucket(bucket_name) == True

    # Write and read object
    with open('/dev/urandom', 'rb') as f:
        object_data = f.read(30 * MEGABYTE)

    h3.write_object(bucket_name, 'largefile', object_data)

    object_info = h3.info_object(bucket_name, 'largefile')
    assert not object_info.is_bad
    assert object_info.size == (30 * MEGABYTE)
    assert type(object_info.creation) == float
    assert type(object_info.last_access) == float
    assert type(object_info.last_modification) == float
    assert type(object_info.last_change) == float

    data = b''
    while True:
        result = h3.read_object(bucket_name, 'largefile', offset=len(data))
        data += result
        if result.done:
            break

    assert data == object_data

    object_info = h3.info_object(bucket_name, 'largefile')
    assert not object_info.is_bad
    assert object_info.size == (30 * MEGABYTE)
    assert type(object_info.creation) == float
    assert type(object_info.last_access) == float
    assert type(object_info.last_modification) == float
    assert type(object_info.last_change) == float

    assert h3.list_objects(bucket_name) == ['largefile']

    # Empty and delete bucket
    assert h3.purge_bucket(bucket_name) == True
    assert h3.delete_bucket(bucket_name) == True

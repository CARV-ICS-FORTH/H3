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

from pyh3 import H3

def pytest_addoption(parser):
    parser.addoption('--config', action='store', help="Specify h3lib's configuration file")
    parser.addoption('--store', action='store', help="Choose which store to use (default: set in config file)")

@pytest.fixture(scope='module')
def h3(request):
    config = request.config.getoption('--config')
    store = request.config.getoption('--store')

    storage_type = None
    if store == 'filesystem':
        storage_type = H3.STORE_FILESYSTEM
    elif store == 'kreon':
        storage_type = H3.STORE_KREON
    elif store == 'rocksdb':
        storage_type = H3.STORE_ROCKSDB
    if not storage_type:
        storage_type = H3.STORE_CONFIG

    assert config, 'You need to specify a configuration file with "--config"'
    return H3(config, storage_type)

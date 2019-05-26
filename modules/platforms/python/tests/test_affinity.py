#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time

from pyignite.api import *
from pyignite.datatypes import *
from pyignite.datatypes.key_value import PeekModes
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import *


def test_get_node_partitions(client):

    conn = client.random_node

    cache_1 = client.get_or_create_cache('test_cache_1')
    cache_2 = client.get_or_create_cache({
        PROP_NAME: 'test_cache_2',
        PROP_CACHE_KEY_CONFIGURATION: [
            {
                'type_name': ByteArray.type_name,
                'affinity_key_field_name': 'byte_affinity',
            }
        ],
    })
    cache_3 = client.get_or_create_cache('test_cache_3')
    cache_4 = client.get_or_create_cache('test_cache_4')
    cache_5 = client.get_or_create_cache('test_cache_5')

    time.sleep(1)

    result = cache_get_node_partitions(
        conn,
        [cache_1.cache_id, cache_2.cache_id]
    )
    assert result.status == 0, result.message


def test_affinity(client):

    cache_1 = client.get_or_create_cache('test_cache_1')
    k = '1234'
    v = 2

    cache_1.put(k, v)

    best_node = cache_1.get_best_node(k)

    for node in client._nodes.values():
        result = cache_local_peek(
            node, cache_1.cache_id, k,
        )
        if node is best_node:
            assert result.value == v
        else:
            assert result.value is None

// Copyright [2019] [FORTH-ICS]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Error codes
#define KV_SUCCESS 1
#define KV_FAILURE 0

typedef void * kv_handle;
typedef char * kv_key;

// Handle management
kv_handle kv_init_handle(kv_name storage, int argc, char **argv);
void kv_free_handle(kv_handle handle);

// Key-value operations
int kv_list(kv_handle handle);
int kv_exists(kv_handle handle, kv_key key);
int kv_read(kv_handle handle, kv_key key, int max_size, int offset, void *value, int *size);
int kv_write(kv_handle handle, kv_key key, void *value, int offset, int size);
int kv_delete(kv_handle handle, kv_key key)
int kv_sync(kv_handle handle)

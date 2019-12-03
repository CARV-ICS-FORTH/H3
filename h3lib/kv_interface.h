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

#ifndef KV_INTERFACE_H_
#define KV_INTERFACE_H_

// Error codes
#define KV_SUCCESS 1
#define KV_FAILURE 0

typedef void* KV_Handle;
typedef char* KV_Key;

//// Handle management
//KV_Handle kv_init_handle(H3_StoreType storeType, void* args);
//void kv_free_handle(KV_Handle handle);

// Key-value operations
typedef struct KV_Operations {
	KV_Handle (*init)(void* params);
	void (*free)(KV_Handle handle);

    int (*metadata_read)(KV_Handle handle, KV_Key key, int maxSize, int offset, void *value, int *size);
    int (*metadata_write)(KV_Handle handle, KV_Key key, void *value, int offset, int size);

	int (*list)(KV_Handle handle);
	int (*exists)(KV_Handle handle, KV_Key key);
	int (*read)(KV_Handle handle, KV_Key key, int maxSize, int offset, void *value, int *size);
	int (*create)(KV_Handle handle, KV_Key key, void *value, int offset, int size);
	int (*write)(KV_Handle handle, KV_Key key, void *value, int offset, int size);
	int (*copy)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	int (*move)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	int (*delete)(KV_Handle handle, KV_Key key);
	int (*sync)(KV_Handle handle);
} KV_Operations;

#endif // KV_INTERFACE_H_



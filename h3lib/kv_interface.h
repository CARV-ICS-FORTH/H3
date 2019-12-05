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

#include <stdint.h>
#include <glib.h>


typedef void* KV_Handle;
typedef char* KV_Key;
typedef unsigned char* KV_Value;

// Error codes
typedef enum {
    KV_FAILURE, KV_KEY_EXIST, KV_KEY_NOT_EXIST, KV_SUCCESS
}KV_Status;


// Key-value operations
typedef struct KV_Operations {
	KV_Handle (*init)(GKeyFile* cfgFile);
	void (*free)(KV_Handle handle);

	/*
	 * --- Read Operations --
	 * For functions metadata_read() and read() argument "size" is in/out, i.e.
	 * the caller sets it with the chunk size to retrieve (0x00 for all) and the
	 * storage-backend sets it to the size it managed to retrieve.
	 *
	 * Also it is the responsibility of the storage-backend to allocate the buffer
	 * for the data and the callers to release it.
	 *
	 * --- Write Operations ---
	 * Write operations create a key if doesn't exist or update its value otherwise. For
	 * functions metadata_write() and write(), argument "size" indicates the size of the
	 * caller supplied value, whereas argument "offset" indicates the starting-position
	 * within the current buffer associated with the key that will be replaced by value.
	 * If the size of the buffer is smaller than the offset the buffer will be padded
	 * with 0x00 to make the offset fit. Padding is applied even if the key is just created.
	 *
	 *
	 * --- Create Operations ---
	 * Creates are identical to Writes but fail if key already exists.
	 *
	 *
	 * --- List Operations ---
	 * This operation accept a pointer to store the list and another to store its size. If
	 * the list pointer is NULL then no list will be generated, i.e. only interested in the
	 * number of keys.
	 */



	KV_Status (*metadata_read)(KV_Handle handle, KV_Key key, uint64_t offset, KV_Value* value, size_t* size);
	KV_Status (*metadata_write)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
	KV_Status (*metadata_create)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
	KV_Status (*metadata_delete)(KV_Handle handle, KV_Key key);

	KV_Status (*list)(KV_Handle handle, KV_Key prefix, KV_Key* key, uint64_t* nKeys);
	KV_Status (*exists)(KV_Handle handle, KV_Key key);
	KV_Status (*read)(KV_Handle handle, KV_Key key, uint64_t offset, KV_Value* value, size_t* size);
	KV_Status (*create)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
	KV_Status (*write)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
	KV_Status (*copy)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	KV_Status (*move)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	KV_Status (*delete)(KV_Handle handle, KV_Key key);
	KV_Status (*sync)(KV_Handle handle);
} KV_Operations;

#endif /* KV_INTERFACE_H_ */



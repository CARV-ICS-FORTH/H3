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

#include <glib.h>

// Error codes
#define KV_SUCCESS 1
#define KV_FAILURE 0

typedef void* KV_Handle;
typedef char* KV_Key;
typedef unsigned char* KV_Value;


// Key-value operations
typedef struct KV_Operations {
	KV_Handle (*init)(GKeyFile* cfgFile);
	void (*free)(KV_Handle handle);

	/*
	 * For functions metadata_read() and read() parameter size is in/out, i.e.
	 * the caller sets it with the chunk size to retrieve (0x00 for all) and the
	 * storage-backend sets it to the size it managed to retrieve.
	 *
	 * Also it is the responsibility of the storage-backend to allocate the buffer
	 * for the data and the callers to release it.
	 */

    int (*metadata_read)(KV_Handle handle, KV_Key key, int offset, KV_Value* value, int* size);
    int (*metadata_write)(KV_Handle handle, KV_Key key, KV_Value value, int offset, int size);

	int (*list)(KV_Handle handle);
	int (*exists)(KV_Handle handle, KV_Key key);
	int (*read)(KV_Handle handle, KV_Key key, int offset, KV_Value* value, int* size);
	int (*create)(KV_Handle handle, KV_Key key, KV_Value value, int offset, int size);
	int (*write)(KV_Handle handle, KV_Key key, KV_Value value, int offset, int size);
	int (*copy)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	int (*move)(KV_Handle handle, KV_Key srcKey, KV_Key dstKey);
	int (*delete)(KV_Handle handle, KV_Key key);
	int (*sync)(KV_Handle handle);
} KV_Operations;

#endif /* KV_INTERFACE_H_ */



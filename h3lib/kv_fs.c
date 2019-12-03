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

#include <stdio.h>
#include <stdlib.h>

#include <glib.h>

#include "kv_interface.h"
#include "kv_fs.h"

typedef struct {
    char* root;
}KV_Filesystem_Handle;

KV_Handle kv_init_fs(GKeyFile* cfgFile) {
    g_autoptr(GError) error = NULL;
    KV_Filesystem_Handle* handle = malloc(sizeof(KV_Filesystem_Handle));

    handle->root = g_key_file_get_string (cfgFile, "FS", "root", &error);
    if(handle->root == NULL){

        // Key 'root' is not defined, use default value instead
        if(g_error_matches (error, G_KEY_FILE_ERROR, G_KEY_FILE_ERROR_KEY_NOT_FOUND)){
            handle->root = strdup("/tmp/h3");
        }

        // Error, section 'FS' is missing
        else
            return NULL;
    }

	return (KV_Handle)handle;
}

void kv_free_fs(KV_Handle handle) {
    KV_Filesystem_Handle* iHandle = (KV_Filesystem_Handle*) handle;
    free(iHandle->root);
    free(iHandle);
    return;
}

int kv_list_fs(KV_Handle handle) {
	return KV_SUCCESS;
}

int kv_exists_fs(KV_Handle handle, KV_Key key) {
	return KV_SUCCESS;
}

int kv_read_fs(KV_Handle handle, KV_Key key, int max_size, int offset, void *value, int *size) {
	return KV_SUCCESS;
}

int kv_create_fs(KV_Handle handle, KV_Key key, void *value, int offset, int size) {
    return KV_SUCCESS;
}

int kv_write_fs(KV_Handle handle, KV_Key key, void *value, int offset, int size) {
	return KV_SUCCESS;
}

int kv_copy_fs(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
	return KV_SUCCESS;
}

int kv_move_fs(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
	return KV_SUCCESS;
}

int kv_delete_fs(KV_Handle handle, KV_Key key) {
	return KV_SUCCESS;
}

int kv_sync_fs(KV_Handle handle) {
	return KV_SUCCESS;
}

KV_Operations operationsFilesystem = {
	.init = kv_init_fs,
	.free = kv_free_fs,

    .metadata_read = kv_read_fs,
    .metadata_write = kv_write_fs,

	.list = kv_list_fs,
	.exists = kv_exists_fs,
	.read = kv_read_fs,
	.create = kv_create_fs,
	.write = kv_write_fs,
	.copy = kv_copy_fs,
	.move = kv_move_fs,
	.delete = kv_delete_fs,
	.sync = kv_sync_fs
};

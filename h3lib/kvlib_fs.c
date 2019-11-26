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

#import "kvlib.h"

kv_handle kv_init_fs(int argc, char **argv) {
	return NULL;
}

void kv_free_fs(kv_handle handle) {
	return KV_SUCCESS;
}

int kv_list_fs(kv_handle handle) {
	return KV_SUCCESS;
}

int kv_exists_fs(kv_handle handle, kv_key key) {
	return KV_SUCCESS;
}

int kv_read_fs(kv_handle handle, kv_key key, int max_size, int offset, void *value, int *size) {
	return KV_SUCCESS;
}

int kv_write_fs(kv_handle handle, kv_key key, void *value, int offset, int size) {
	return KV_SUCCESS;
}

int kv_copy_fs(kv_handle handle, kv_key src_key, kv_key dest_key) {
	return KV_SUCCESS;
}

int kv_move_fs(kv_handle handle, kv_key src_key, kv_key dest_key) {
	return KV_SUCCESS;
}

int kv_delete_fs(kv_handle handle, kv_key key) {
	return KV_SUCCESS;
}

int kv_sync_fs(kv_handle handle) {
	return KV_SUCCESS;
}

const struct kv_operations kv_operations_filesystem = {
	.init = kv_init_fs,
	.free = kv_free_fs,
	.list = kv_list_fs,
	.exists = kv_exists_fs,
	.read = kv_read_fs,
	.write = kv_write_fs,
	.copy = kv_copy_fs,
	.move = kv_move_fs,
	.delete = kv_delete_fs,
	.sync = kv_sync_fs
};

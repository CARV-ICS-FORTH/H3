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
#define H3_SUCCESS 1
#define H3_FAILURE 0

// Types
typedef void * h3_handle;
typedef char * h3_name;
typedef void * h3_multipart;

typedef struct {
	h3_name name;
	int byte_size;
	int item_count;
	int last_access;
	int last_modification;
} h3_bucket_info;

typedef struct {
	h3_name name;
	int byte_size;
	int last_access;
	int last_modification;
} h3_object_info;

typedef struct {
	int position;
	int byte_size;
} h3_multipart_info;

typedef void (*h3_name_iterator_cb)(h3_name name, void *user_data);

// Handle management
h3_handle h3_init_handle(h3_name storage, int argc, char **argv);
void h3_free_handle(h3_handle handle);

// Bucket management
int h3_list_buckets(h3_handle handle, int max_size, int offset, h3_name *bucket_names, int *size);
int h3_foreach_bucket(h3_handle handle, h3_name_iterator_cb function, void *user_data);
int h3_info_bucket(h3_handle handle, h3_name bucket_name, h3_bucket_info *bucket_info);
int h3_create_bucket(h3_handle handle, h3_name bucket_name);
int h3_delete_bucket(h3_handle handle, h3_name bucket_name);

// Object management
int h3_list_objects(h3_handle handle, h3_name bucket_name, h3_name prefix, int max_size, int offset, h3_name *object_names, int *size);
int h3_foreach_object(h3_handle handle, h3_name bucket_name, h3_name prefix, int max_size, int offset, h3_name_iterator_cb function, void *user_data);
int h3_info_object(h3_handle handle, h3_name bucket_name, h3_name object_name, h3_object_info *object_info);
int h3_read_object(h3_handle handle, h3_name bucket_name, h3_name object_name, int max_size, int offset, void *data, int *size);
int h3_write_object(h3_handle handle, h3_name bucket_name, h3_name object_name, void *data, int offset, int size);
int h3_copy_object(h3_handle handle, h3_name bucket_name, h3_name src_object_name, h3_name dest_object_name);
int h3_copy_object_part(h3_handle handle, h3_name bucket_name, h3_name src_object_name, int offset, int size, h3_name dest_object_name);
int h3_move_object(h3_handle handle, h3_name bucket_name, h3_name src_object_name, h3_name dest_object_name);
int h3_delete_object(h3_handle handle, h3_name bucket_name, h3_name object_name);

// Multipart management
int h3_list_multiparts(h3_handle handle, h3_name bucket_name, int max_size, int offset, h3_multipart *multiparts, int *size);
int h3_create_multipart(h3_handle handle, h3_name bucket_name, h3_multipart *multipart);
int h3_delete_multipart(h3_handle handle, h3_name bucket_name, h3_multipart multipart);

int h3_info_multipart(h3_handle handle, h3_name bucket_name, h3_multipart multipart, int max_size, int offset, h3_multipart_info *multipart_info, int *size);
int h3_write_multipart(h3_handle handle, h3_name bucket_name, h3_multipart multipart, int position, void *data, int size);
int h3_copy_multipart(h3_handle handle, h3_name bucket_name, h3_name object_name, int offset, int size, h3_multipart multipart, int position);
int h3_commit_multipart(h3_handle handle, h3_name bucket_name, h3_name object_name, h3_multipart multipart);

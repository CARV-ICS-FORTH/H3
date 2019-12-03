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
typedef void* H3_Handle;
typedef char* H3_Name;
typedef void* H3_MultipartId;

typedef enum {
    H3_STORE_REDIS, H3_STORE_ROCKSDB, H3_STORE_KREON, H3_STORE_FILESYSTEM, H3_STORE_IME, H3_STORE_NumOfStores
}H3_StoreType;

typedef struct {
	H3_Name name;
	int byte_size;
	int item_count;
	int last_access;
	int last_modification;
} H3_BucketInfo;

typedef struct {
	H3_Name name;
	int byte_size;
	int last_access;
	int last_modification;
} H3_ObjectInfo;

typedef struct {
	int position;
	int byte_size;
} H3_MultipartInfo;

typedef void (*h3_name_iterator_cb)(H3_Name name, void* userData);

char* H3_Version();

// Handle management
H3_Handle H3_Init(H3_StoreType storageType, char* cfgFileName);
void H3_Free(H3_Handle handle);

// Bucket management
int H3_ListBuckets(H3_Handle handle, int maxSize, int offset, H3_Name* bucketNames, int* size);
int H3_ForeachBucket(H3_Handle handle, h3_name_iterator_cb function, void* userData);
int H3_InfoBucket(H3_Handle handle, H3_Name bucketName, H3_BucketInfo* bucketInfo);
int H3_CreateBucket(H3_Handle handle, H3_Name bucketName);
int H3_DeleteBucket(H3_Handle handle, H3_Name bucketName);

// Object management
int H3_ListObjects(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_Name* objectNames, int* size);
int H3_ForeachObject(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, h3_name_iterator_cb function, void* userData);
int H3_InfoObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo);
int H3_ReadObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, int maxSize, int offset, void* data, int* size);
int H3_WriteObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, void* data, int offset, int size);
int H3_CopyObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
int H3_PartialCopyObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, int offset, int size, H3_Name dstObjectName);
int H3_MoveObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
int H3_DeleteObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName);

// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_MultipartId* idArray, int* size);
int H3_CreateMultipart(H3_Handle handle, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id);
int H3_CompleteMultipart(H3_Handle handle, H3_MultipartId id);
int H3_AbortMultipart(H3_Handle handle, H3_MultipartId id);
int H3_ListParts(H3_Handle handle, H3_MultipartId id, int maxSize, int offset, H3_MultipartInfo* info, int* size);
int H3_UploadPart(H3_Handle handle, H3_MultipartId id, int partNumber, void* data, int size);
int H3_UploadPartCopy(H3_Handle handle, H3_Name objectName, int offset, int size, H3_MultipartId id, int partNumber);

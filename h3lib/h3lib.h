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

#ifndef H3LIB_H_
#define H3LIB_H_

#include <stdint.h>
#include <time.h>

// Error codes
#define H3_SUCCESS 1
#define H3_FAILURE 0

#define H3_ERROR_MESSAGE    512

#define H3_BUCKET_NAME_SIZE 64
#define H3_OBJECT_NAME_SIZE 512

// Types
typedef void* H3_Handle;
typedef char* H3_Name;
typedef void* H3_MultipartId;

typedef enum {
    H3_STORE_REDIS, H3_STORE_ROCKSDB, H3_STORE_KREON, H3_STORE_FILESYSTEM, H3_STORE_IME, H3_STORE_NumOfStores
}H3_StoreType;

typedef struct{
    int userId;
}H3_Token;

typedef struct {
    H3_Name name;
    size_t size;
    uint64_t nObjects;
    time_t creation;
} H3_BucketInfo;

typedef struct {
    H3_Name name;
    char isBad;
    size_t size;
    time_t lastAccess;
    time_t lastModification;
} H3_ObjectInfo;

typedef struct {
    uint32_t partId;
    size_t size;
} H3_MultipartInfo;

typedef void (*h3_name_iterator_cb)(H3_Name name, void* userData);

char* H3_Version();

// Handle management
H3_Handle H3_Init(H3_StoreType storageType, char* cfgFileName);
void H3_Free(H3_Handle handle);

// Bucket management
int H3_ListBuckets(H3_Handle handle, H3_Token* token, H3_Name* bucketNames, size_t* size);
int H3_ForeachBucket(H3_Handle handle, H3_Token* token, h3_name_iterator_cb function, void* userData);
int H3_InfoBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_BucketInfo* bucketInfo);
int H3_CreateBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName);
int H3_DeleteBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName);

// Object management
int H3_ListObjects(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_Name* objectNames, size_t* size);
int H3_ForeachObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, h3_name_iterator_cb function, void* userData);
int H3_InfoObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo);
int H3_CreateObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, size_t offset);
int H3_ReadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, size_t offset, void* data, size_t* size);
int H3_WriteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, uint64_t offset, void* data, size_t size);
int H3_CopyObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
int H3_CopyObjectRange(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, uint64_t offset, size_t size, H3_Name dstObjectName);
int H3_MoveObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
int H3_DeleteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName);

// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_MultipartId* idArray, size_t* size);
int H3_CreateMultipart(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id);
int H3_CompleteMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id);
int H3_AbortMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id);
int H3_ListParts(H3_Handle handle, H3_Token* token, H3_MultipartId id, size_t maxSize, uint64_t offset, H3_MultipartInfo* info, size_t* size);
int H3_UploadPart(H3_Handle handle, H3_Token* token, H3_MultipartId id, uint32_t partNumber, void* data, size_t size);
int H3_UploadPartCopy(H3_Handle handle, H3_Token* token, H3_Name objectName, uint64_t offset, size_t size, H3_MultipartId id, uint32_t partNumber);

#endif /* H3LIB_H_ */

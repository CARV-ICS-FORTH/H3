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

#include "h3lib.h"
#include "h3lib_config.h"
#include "kv_interface.h"
#include "kv_fs.h"

#define BUFF_SIZE 24

typedef struct {
    int dummy;
    H3_StoreType type;

    // Store specific
    KV_Handle handle;
    KV_Operations* operation;
}H3_Context;

extern KV_Operations operationsFilesystem;

char* H3_Version(){
    static char buffer[BUFF_SIZE];
    snprintf(buffer, BUFF_SIZE, "v%d.%d\n", H3LIB_VERSION_MAJOR, H3LIB_VERSION_MINOR);
    return buffer;
}


// Handle management
H3_Handle H3_Init(H3_StoreType storageType, void* storageParams) {
    H3_Context* context = malloc(sizeof(H3_Context));
    void* params;

    switch(storageType){
        case H3_STORE_REDIS:
        case H3_STORE_ROCKSDB:
        case H3_STORE_KREON:
        case H3_STORE_IME:
            context->operation = NULL;
            break;

        case H3_STORE_FILESYSTEM: {
            KV_Filesystem_Params* fsParams = malloc(sizeof(KV_Filesystem_Params));
            // Extract info from storageParams
            fsParams->dummy = 0;
            context->operation = &operationsFilesystem;
            params = (void*) fsParams;
        }
        break;

        default:
            exit(-1);
    }


    context->handle = context->operation->init(params);

    return (H3_Handle)context;
}

void H3_Free(H3_Handle handle){
    H3_Context* context = (H3_Context*)handle;
    context->operation->free(context->handle);
    free(context);
};

// Bucket management
int H3_ListBuckets(H3_Handle handle, int maxSize, int offset, H3_Name* bucketNames, int* size){return 0;}
int H3_ForeachBucket(H3_Handle handle, h3_name_iterator_cb function, void* userData){return 0;}
int H3_InfoBucket(H3_Handle handle, H3_Name bucketName, H3_BucketInfo* bucketInfo){return 0;}
int H3_CreateBucket(H3_Handle handle, H3_Name bucketName){return 0;}
int H3_DeleteBucket(H3_Handle handle, H3_Name bucketName){return 0;}

// Object management
int H3_ListObjects(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_Name* objectNames, int* size){return 0;}
int H3_ForeachObject(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, h3_name_iterator_cb function, void* userData){return 0;}
int H3_InfoObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){return 0;}
int H3_ReadObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, int maxSize, int offset, void* data, int* size){return 0;}
int H3_WriteObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName, void* data, int offset, int size){return 0;}
int H3_CopyObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return 0;}
int H3_PartialCopyObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, int offset, int size, H3_Name dstObjectName){return 0;}
int H3_MoveObject(H3_Handle handle, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return 0;}
int H3_DeleteObject(H3_Handle handle, H3_Name bucketName, H3_Name objectName){return 0;}

// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_MultipartId* idArray, int* size){return 0;}
int H3_CreateMultipart(H3_Handle handle, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id){return 0;}
int H3_CompleteMultipart(H3_Handle handle, H3_MultipartId id){return 0;}
int H3_AbortMultipart(H3_Handle handle, H3_MultipartId id){return 0;}
int H3_ListParts(H3_Handle handle, H3_MultipartId id, int maxSize, int offset, H3_MultipartInfo* info, int* size){return 0;}
int H3_UploadPart(H3_Handle handle, H3_MultipartId id, int partNumber, void* data, int size){return 0;}
int H3_UploadPartCopy(H3_Handle handle, H3_Name objectName, int offset, int size, H3_MultipartId id, int partNumber){return 0;}

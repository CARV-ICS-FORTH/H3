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

#include "h3lib.h"
#include "h3lib_config.h"
#include "kv_interface.h"

#define BUFF_SIZE 24
#define H3_SYSTEM_ID    0x00

#define mGetUserId(token)   (token)->userId

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
// https://developer.gnome.org/glib/stable/glib-Key-value-file-parser.html
H3_Handle H3_Init(H3_StoreType storageType, char* cfgFileName) {
    g_autoptr(GError) error = NULL;
    GKeyFile* cfgFile = g_key_file_new();
    if( !g_key_file_load_from_file(cfgFile, cfgFileName, G_KEY_FILE_NONE, &error)) {
//        if(errorMsg)
//            snprintf(errorMsg, H3_ERROR_MESSAGE, "Error loading key file: %s", error->message);
        return NULL;
    }


    H3_Context* context = malloc(sizeof(H3_Context));
    switch(storageType){
        case H3_STORE_REDIS:
        case H3_STORE_ROCKSDB:
        case H3_STORE_KREON:
        case H3_STORE_IME:
            context->operation = NULL;
            break;

        case H3_STORE_FILESYSTEM:
            context->operation = &operationsFilesystem;
            break;

        default:
//            if(errorMsg)
//                snprintf(errorMsg, H3_ERROR_MESSAGE, "Unexpected storage type: %d", storageType);
            free(context);
            return NULL;
    }

    context->type = storageType;
    context->handle = context->operation->init(cfgFile);

    return (H3_Handle)context;
}

void H3_Free(H3_Handle handle){
    H3_Context* context = (H3_Context*)handle;
    context->operation->free(context->handle);
    free(context);
};

// Bucket management
int H3_ListBuckets(H3_Handle handle, H3_Token* token, int maxSize, int offset, H3_Name* bucketNames, int* size){return H3_FAILURE;}
int H3_ForeachBucket(H3_Handle handle, H3_Token* token, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}
int H3_InfoBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_BucketInfo* bucketInfo){return H3_FAILURE;}

int H3_CreateBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){
    int userId;

    // Validate bucketName

    // Extract userId from token
    if( !(userId = mGetUserId(token)))
        return H3_FAILURE;

//    user_metadata = get(key=user_id)
//    create(key=bucket_id, value=bucket_metadata)
//    user_metadata += bucket_id
//    put(key=user_id, value=user_metadata)
    return H3_SUCCESS;
}

int H3_DeleteBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){return H3_FAILURE;}

// Object management
int H3_ListObjects(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_Name* objectNames, int* size){return H3_FAILURE;}
int H3_ForeachObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}
int H3_InfoObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){return H3_FAILURE;}
int H3_ReadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, int maxSize, int offset, void* data, int* size){return H3_FAILURE;}
int H3_WriteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, int offset, int size){return H3_FAILURE;}
int H3_CopyObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_CopyObjectRange(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, int offset, int size, H3_Name dstObjectName){return H3_FAILURE;}
int H3_MoveObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_DeleteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName){return H3_FAILURE;}

// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, int maxSize, int offset, H3_MultipartId* idArray, int* size){return H3_FAILURE;}
int H3_CreateMultipart(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id){return H3_FAILURE;}
int H3_CompleteMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_AbortMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_ListParts(H3_Handle handle, H3_Token* token, H3_MultipartId id, int maxSize, int offset, H3_MultipartInfo* info, int* size){return H3_FAILURE;}
int H3_UploadPart(H3_Handle handle, H3_Token* token, H3_MultipartId id, int partNumber, void* data, int size){return H3_FAILURE;}
int H3_UploadPartCopy(H3_Handle handle, H3_Token* token, H3_Name objectName, int offset, int size, H3_MultipartId id, int partNumber){return H3_FAILURE;}

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

#include "common.h"

extern KV_Operations operationsFilesystem;

int GetUserId(H3_Token* token, H3_UserId id){
    snprintf(id, H3_USERID_SIZE, "@%d", token->userId);
    return H3_SUCCESS;
}

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


    H3_Context* ctx = malloc(sizeof(H3_Context));
    switch(storageType){
        case H3_STORE_REDIS:
        case H3_STORE_ROCKSDB:
        case H3_STORE_KREON:
        case H3_STORE_IME:
            ctx->operation = NULL;
            break;

        case H3_STORE_FILESYSTEM:
            ctx->operation = &operationsFilesystem;
            break;

        default:
//            if(errorMsg)
//                snprintf(errorMsg, H3_ERROR_MESSAGE, "Unexpected storage type: %d", storageType);
            free(ctx);
            return NULL;
    }

    ctx->type = storageType;
    ctx->handle = ctx->operation->init(cfgFile);

    return (H3_Handle)ctx;
}

void H3_Free(H3_Handle handle){
    H3_Context* ctx = (H3_Context*)handle;
    ctx->operation->free(ctx->handle);
    free(ctx);
};



// Object management
int H3_ListObjects(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_Name* objectNames, size_t* size){return H3_FAILURE;}
int H3_ForeachObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}
int H3_InfoObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){return H3_FAILURE;}
int H3_ReadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, size_t maxSize, uint64_t offset, void* data, size_t* size){return H3_FAILURE;}
int H3_WriteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, uint64_t offset, size_t size){return H3_FAILURE;}
int H3_CopyObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_CopyObjectRange(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, uint64_t offset, size_t size, H3_Name dstObjectName){return H3_FAILURE;}
int H3_MoveObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_DeleteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName){return H3_FAILURE;}

// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_MultipartId* idArray, size_t* size){return H3_FAILURE;}
int H3_CreateMultipart(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id){return H3_FAILURE;}
int H3_CompleteMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_AbortMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_ListParts(H3_Handle handle, H3_Token* token, H3_MultipartId id, size_t maxSize, uint64_t offset, H3_MultipartInfo* info, size_t* size){return H3_FAILURE;}
int H3_UploadPart(H3_Handle handle, H3_Token* token, H3_MultipartId id, uint32_t partNumber, void* data, size_t size){return H3_FAILURE;}
int H3_UploadPartCopy(H3_Handle handle, H3_Token* token, H3_Name objectName, uint64_t offset, size_t size, H3_MultipartId id, uint32_t partNumber){return H3_FAILURE;}

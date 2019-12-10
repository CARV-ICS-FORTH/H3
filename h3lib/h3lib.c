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

void GetObjectId(H3_Name bucketName, H3_Name objectName, H3_ObjectId id){
    snprintf(id,sizeof(H3_ObjectId), "%s/%s", bucketName, objectName);
}

int GetBucketIndex(H3_UserMetadata* userMetadata, H3_Name bucketName){
    int i;
    for(i=0; i<userMetadata->nBuckets && strcmp(userMetadata->bucket[i], bucketName); i++);
    return i;
}

char* H3_Version(){
    static char buffer[BUFF_SIZE];
    snprintf(buffer, BUFF_SIZE, "v%d.%d\n", H3LIB_VERSION_MAJOR, H3LIB_VERSION_MINOR);
    return buffer;
}

char* GetPartId(H3_ObjectId objId, int partNumber, int subPartNumber){
    char* key = NULL;
    gchar* hash = g_compute_checksum_for_string(G_CHECKSUM_SHA256, objId, -1);

    if(partNumber >= 0){
        if(subPartNumber >= 0){
            asprintf(&key,"_%s#%d.%d", hash, partNumber, subPartNumber);
        }
        else{
            asprintf(&key,"_%s#%d", hash, partNumber);
        }
    }
    else {
        asprintf(&key,"_%s", hash);
    }
    g_free(hash);
    return key;
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





// Multipart management
int H3_ListMultiparts(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_MultipartId* idArray, size_t* size){return H3_FAILURE;}
int H3_CreateMultipart(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* id){return H3_FAILURE;}
int H3_CompleteMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_AbortMultipart(H3_Handle handle, H3_Token* token, H3_MultipartId id){return H3_FAILURE;}
int H3_ListParts(H3_Handle handle, H3_Token* token, H3_MultipartId id, size_t maxSize, uint64_t offset, H3_MultipartInfo* info, size_t* size){return H3_FAILURE;}
int H3_UploadPart(H3_Handle handle, H3_Token* token, H3_MultipartId id, uint32_t partNumber, void* data, size_t size){return H3_FAILURE;}
int H3_UploadPartCopy(H3_Handle handle, H3_Token* token, H3_Name objectName, uint64_t offset, size_t size, H3_MultipartId id, uint32_t partNumber){return H3_FAILURE;}

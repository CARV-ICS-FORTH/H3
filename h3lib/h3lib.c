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

int GetUserId(H3_Token token, H3_UserId id){
    snprintf(id, H3_USERID_SIZE, "@%d", token->userId);
    return TRUE;
}

int GetBucketId(H3_Name bucketName, H3_BucketId id){
    snprintf(id, H3_BUCKET_NAME_SIZE+1, "#%s", bucketName);
    return TRUE;
}

void GetObjectId(H3_Name bucketName, H3_Name objectName, H3_ObjectId id){

    // Common usage
    if(objectName)
        snprintf(id,sizeof(H3_ObjectId), "%s/%s", bucketName, objectName);

    // Used in ListObjects
    else
        snprintf(id,sizeof(H3_ObjectId), "%s/", bucketName);
}


H3_MultipartId GeneratetMultipartId(uuid_t uuid ){
    H3_MultipartId id = malloc(H3_MULIPARTID_SIZE);
    H3_UUID uuidString;
    uuid_unparse_lower(uuid, uuidString);
    snprintf(id, H3_MULIPARTID_SIZE, "%s$", uuidString);
    return id;
}


void GetMultipartObjectId(H3_Name bucketName, H3_Name objectName, H3_ObjectId id){

    // Common usage
    if(objectName)
        snprintf(id,sizeof(H3_ObjectId), "%s$%s", bucketName, objectName);

    // Used in ListMultiparts
    else
        snprintf(id,sizeof(H3_ObjectId), "%s$", bucketName);
}

char* ConvertToOdrinary(H3_ObjectId id){
    char* marker = strchr(id, '$');
    *marker = '/';
    return id;
}

char* GetBucketFromId(H3_ObjectId objId, H3_BucketId bucketId){
    char* marker = strchr(objId, '$');
    if(!marker)
        marker = strchr(objId, '/');

    size_t size = min(marker - objId, H3_BUCKET_NAME_SIZE);
    memcpy(bucketId, objId, size);
    bucketId[size] = '\0';
    return bucketId;
}


int GetBucketIndex(H3_UserMetadata* userMetadata, H3_Name bucketName){
    int i;
    for(i=0; i<userMetadata->nBuckets && strcmp(userMetadata->bucket[i], bucketName); i++);
    return i;
}

#define H3_VERSION_SIZE 12
char* H3_Version(){
    static char buffer[H3_VERSION_SIZE];
    snprintf(buffer, H3_VERSION_SIZE, "v%d.%d", H3LIB_VERSION_MAJOR, H3LIB_VERSION_MINOR);
    return buffer;
}

void CreatePartId(H3_PartId partId, uuid_t uuid, int partNumber, int subPartNumber){
    H3_UUID uuidString;
    uuid_unparse_lower(uuid, uuidString);

    if(partNumber >= 0){
        if(subPartNumber >= 0)
            snprintf(partId, sizeof(H3_PartId), "_%s#%d.%d", uuidString, partNumber, subPartNumber);
        else
            snprintf(partId, sizeof(H3_PartId), "_%s#%d", uuidString, partNumber);

    }
    else
        snprintf(partId, sizeof(H3_PartId), "_%s", uuidString);
}


/*
 * Although the speck dictates that single-part objects will not have the part post-fixed with a part-number
 * identifier we append a part-number to all parts since it will be complicated to rename a part according
 * to its object's ever changing size.
 */

char* PartToId(H3_PartId partId, uuid_t uuid, H3_PartMetadata* part){
    H3_UUID uuidString;
    uuid_unparse_lower(uuid, uuidString);

    if(part->number >= 0){
        if(part->subNumber >= 0)
            snprintf(partId, sizeof(H3_PartId), "_%s#%d.%d", uuidString, part->number, part->subNumber);
        else
            snprintf(partId, sizeof(H3_PartId), "_%s#%d", uuidString, part->number);
    }
    else
        snprintf(partId, sizeof(H3_PartId), "_%s", uuidString);

    return partId;
}


// http://man7.org/linux/man-pages/man3/uuid_unparse.3.html
//char* UUID2String(uuid_t uuid){
//    static H3_UUID string;
//    uuid_unparse_lower(uuid, string);
//    return string;
//}

int GrantBucketAccess(H3_UserId id, H3_BucketMetadata* meta){
    return !strncmp(id, meta->userId, sizeof(H3_UserId));
}

int GrantObjectAccess(H3_UserId id, H3_ObjectMetadata* meta){
    return !strncmp(id, meta->userId, sizeof(H3_UserId));
}

int GrantMultipartAccess(H3_UserId id, H3_MultipartMetadata* meta){
    return !strncmp(id, meta->userId, sizeof(H3_UserId));
}

H3_StoreType GetStoreType(GKeyFile* cfgFile){
    H3_StoreType store = H3_STORE_CONFIG;
    char* strStore = g_key_file_get_string (cfgFile, "H3", "store", NULL);
    if(strStore){
        if(     strcmp(strStore, "filesystem") == 0)     store = H3_STORE_FILESYSTEM;
        else if(strcmp(strStore, "kreon") == 0)          store = H3_STORE_KREON;
        else if(strcmp(strStore, "rocksdb") == 0)        store = H3_STORE_ROCKSDB;
        else if(strcmp(strStore, "redis") == 0)          store = H3_STORE_REDIS;
        else if(strcmp(strStore, "ime") == 0)            store = H3_STORE_IME;
    }

    return store;
}



/*! Initialize library
 * @param[in] storageType   The storage provider to be used with this instance
 * @param[in] cfgFileName   The configuration file containing provider specific information
 * @result  The handle if connected to provider, NULL otherwise.
 */
// https://developer.gnome.org/glib/stable/glib-Key-value-file-parser.html
H3_Handle H3_Init(H3_StoreType storageType, char* cfgFileName) {
    g_autoptr(GError) error = NULL;
    GKeyFile* cfgFile = g_key_file_new();
    // printf("Filename %s\n",cfgFileName);
    if( !g_key_file_load_from_file(cfgFile, cfgFileName, G_KEY_FILE_NONE, &error)) {
        return NULL;
    }

    if(storageType == H3_STORE_CONFIG){
        storageType = GetStoreType(cfgFile);
    }

    H3_Context* ctx = malloc(sizeof(H3_Context));
    switch(storageType){
        case H3_STORE_REDIS:
            printf("Using REDIS driver...\n");
            ctx->operation = NULL;
            break;

        case H3_STORE_ROCKSDB:
            printf("Using ROCKSDB driver...\n");
            ctx->operation = NULL;
            break;

        case H3_STORE_KREON:
            printf("Using KREON driver...\n");
            ctx->operation = NULL;
            break;

        case H3_STORE_IME:
            printf("Using IME driver...\n");
            ctx->operation = NULL;
            break;

        case H3_STORE_FILESYSTEM:
            printf("Using kv_fs driver...\n");
            ctx->operation = &operationsFilesystem;
            break;

        default:
            printf("WARNING wrong driver name\n");
            free(ctx);
            return NULL;
    }

    ctx->type = storageType;
    ctx->handle = ctx->operation->init(cfgFile);

    return (H3_Handle)ctx;
}


/*! \brief Destroy an h3lib handle
 * Deallocates buffers and renders handle inoperable. Using the handle after it has
 * been freed leads to unexpected behavior.
 * @param[in] handle A handle previously generated by H3_Init()
 */
void H3_Free(H3_Handle handle){
    H3_Context* ctx = (H3_Context*)handle;
    ctx->operation->free(ctx->handle);
    free(ctx);
};



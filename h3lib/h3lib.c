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
#include "util.h"
#include "url_parser.h"

extern KV_Operations operationsFilesystem;

#ifdef H3LIB_USE_REDIS
extern KV_Operations operationsRedis;
#endif

#ifdef H3LIB_USE_KREON
extern KV_Operations operationsKreon;
#endif

#ifdef H3LIB_USE_KREON_RDMA
extern KV_Operations operationsKreonRDMA;
#endif

#ifdef H3LIB_USE_ROCKSDB
extern KV_Operations operationsRocksDB;
#endif

/*
    http://man7.org/linux/man-pages/man2/umask.2.html
    http://man7.org/linux/man-pages/man2/chmod.2.html
    https://en.wikipedia.org/wiki/Umask

	0 - no access to the file
	1 - execute only
	2 - write only
	3 - write and execute
	4 - read only
	5 - read and execute
	6 - read and write
	7 - read, write and execute (full permissions)

	Assign typical values assuming a umask of 022
*/
void InitMode(H3_ObjectMetadata* objMeta){
	size_t length = strlen(objMeta->userId);
	if(objMeta->userId[length - 1] == '/')
		objMeta->mode = S_IFDIR | 0755;
	else
		objMeta->mode = S_IFREG | 0644;
}

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

void GetObjectMetadataId(H3_ObjectMetadataId metadataId, H3_Name bucketName, H3_Name objectName, H3_Name metadataName){
    // Common usage
    if (bucketName && objectName && metadataName)
        snprintf(metadataId, sizeof(H3_ObjectMetadataId), "%s#%s#%s", bucketName, objectName, metadataName);
    // Used to filter objects metadata
    else if (bucketName && objectName)
        snprintf(metadataId, sizeof(H3_ObjectMetadataId), "%s#%s#", bucketName, objectName);
    // Used for list metadata
    else if (bucketName)
        snprintf(metadataId, sizeof(H3_ObjectMetadataId), "%s#", bucketName);
}

H3_Name GenerateDummyObjectName() {
    uuid_t uuid;
    H3_UUID uuidString;
    H3_Name id = malloc(H3_OBJECT_NAME_SIZE);

    uuid_generate(uuid);
    uuid_unparse_lower(uuid, uuidString);
    
    snprintf(id, H3_OBJECT_NAME_SIZE, "%s", uuidString);

    return id;
}

H3_MultipartId GenerateMultipartId(uuid_t uuid ){
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

void GetBucketAndObjectFromId(H3_Name* bucketName, H3_Name* objectName, H3_ObjectId id) {
    char* marker = strchr(id, '/');
    if (marker) {
        size_t bucketNameSize = (size_t)(marker - id);

        *bucketName = (H3_Name)calloc(1, bucketNameSize);
        memcpy(*bucketName, id, bucketNameSize);

        *objectName = (H3_Name)calloc(1, H3_OBJECT_NAME_SIZE);
        memcpy(*objectName, marker + 1, H3_OBJECT_NAME_SIZE);
    }
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

H3_StoreType H3_String2Type(const char* type){
	H3_StoreType store = -1;

    if(type){
        if(     strcmp(type, "file") == 0)           store = H3_STORE_FILESYSTEM;
        else if(strcmp(type, "kreon") == 0)          store = H3_STORE_KREON;
        else if(strcmp(type, "kreon-rdma") == 0)     store = H3_STORE_KREON_RDMA;
        else if(strcmp(type, "rocksdb") == 0)        store = H3_STORE_ROCKSDB;
        else if(strcmp(type, "redis") == 0)          store = H3_STORE_REDIS;
    }

    return store;
}


const char* const StoreType[] = {"file", "kreon", "kreon-rdma", "rocksdb", "redis", "unknown"};
const char* H3_Type2String(H3_StoreType type){
	const char* string;

	if (type < 0 || type >= H3_NumOfStores)
		string = StoreType[H3_NumOfStores];
	else
		string = StoreType[type];

	return string;
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

/*! Initialize library
 * @param[in] storageUri    The storage provider URI to be used with this instance
 * @result  The handle if connected to provider, NULL otherwise.
 */
H3_Handle H3_Init(const char* storageUri) {
    struct parsed_url *url = parse_url(storageUri);
    if (url == NULL) {
        LogActivity(H3_ERROR_MSG, "ERROR: Unrecognized storage URI\n");
        return NULL;
    }
    H3_StoreType storageType = H3_String2Type(url->scheme);
    parsed_url_free(url);

    H3_Context* ctx = malloc(sizeof(H3_Context));

    if(ctx){
		switch(storageType){
			case H3_STORE_FILESYSTEM:
				LogActivity(H3_INFO_MSG, "Using kv_fs driver...\n");
				ctx->operation = &operationsFilesystem;
				break;

			case H3_STORE_ROCKSDB:
#ifdef H3LIB_USE_ROCKSDB
				LogActivity(H3_INFO_MSG, "Using kv_rocksdb driver...\n");
				ctx->operation = &operationsRocksDB;
#else
				LogActivity(H3_INFO_MSG, "WARNING: Driver not available...\n");
				ctx->operation = NULL;
#endif
				break;

			case H3_STORE_KREON:
#ifdef H3LIB_USE_KREON
				LogActivity(H3_INFO_MSG, "Using kv_kreon driver...\n");
				ctx->operation = &operationsKreon;
#else
				LogActivity(H3_INFO_MSG, "WARNING: Driver not available...\n");
				ctx->operation = NULL;
#endif
				break;

            case H3_STORE_KREON_RDMA:
#ifdef H3LIB_USE_KREON_RDMA
                LogActivity(H3_INFO_MSG, "Using kv_kreon_rdma driver...\n");
                ctx->operation = &operationsKreonRDMA;
#else
                LogActivity(H3_INFO_MSG, "WARNING: Driver not available...\n");
                ctx->operation = NULL;
#endif
                break;

            case H3_STORE_REDIS:
#ifdef H3LIB_USE_REDIS
                LogActivity(H3_INFO_MSG, "Using kv_redis driver...\n");
                ctx->operation = &operationsRedis;
                break;
#else
                LogActivity(H3_INFO_MSG, "WARNING: Driver not available...\n");
                ctx->operation = NULL;
#endif

			default:
				LogActivity(H3_ERROR_MSG, "ERROR: Driver not recognized\n");
				ctx->operation = NULL;
				return NULL;
		}


		if(!ctx->operation || !(ctx->handle = ctx->operation->init(storageUri))){
			free(ctx);
			ctx = NULL;
			LogActivity(H3_ERROR_MSG, "ERROR: Failed to initialize storage\n");
		}
		else
			ctx->type = storageType;
    }

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

/*! \brief Retrieve information about the H3 storage
 *
 * @param[in]    handle             An h3lib handle
 * @param[inout] storageInfo        User allocated structure to be filled with the info
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Storage provider error
 */
 H3_Status H3_InfoStorage(H3_Handle handle, H3_StorageInfo* storageInfo) {
    H3_Context* ctx = (H3_Context*)handle;
    
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_Status status = H3_SUCCESS;
    KV_StorageInfo info;
    if (op->storage_info) {
        if (op->storage_info(_handle, &info) == KV_SUCCESS) {
            storageInfo->totalSpace = info.totalSpace;
	        storageInfo->freeSpace  = info.freeSpace;
	        storageInfo->usedSpace  = info.usedSpace;

            status = H3_SUCCESS;
        } else {
            status = H3_FAILURE;
        }
    } else {
        status = H3_FAILURE;
    }

    return status;
 }




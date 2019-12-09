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

typedef KV_Status (*UploadData)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
typedef KV_Status (*UploadMetadata)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);


int ValidateObjectName(char* name){
    regex_t regex;

    if( strnlen(name, H3_OBJECT_NAME_SIZE) == H3_OBJECT_NAME_SIZE   ||          // Too big
        regcomp(&regex, "(^/)|([^/_0-9a-zA-Z-.])|(/{2,})", 0)       ||          // Contains invalid characters, starts with "/", has back-to-back "/"
        regexec(&regex, name, 0, NULL, 0) == REG_NOERROR                ){

        return H3_FAILURE;
    }

    return H3_SUCCESS;
}


static int UploadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, char isCreate, void* data, size_t size, size_t offset){
    int status = H3_FAILURE;

    // Argument check. Note we allow zero-sized objects.
    if(!handle || !token  || !bucketName || !objectName ){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    KV_Status storeStatus = KV_SUCCESS;

    H3_UserId userId;
    H3_ObjectId objId;

    UploadData uploadData;
    UploadMetadata uploadMetadata;
    if(isCreate){
        uploadData = op->create;
        uploadMetadata = op->metadata_create;
    }
    else {
        uploadData = op->write;
        uploadMetadata = op->metadata_write;
    }

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, objectName, objId);

    // Allocate & populate Object metadata
    uint nParts = (offset + size + H3_PART_SIZE - 1)/H3_PART_SIZE;
    size_t objMetaSize = sizeof(H3_ObjectMetadata) + nParts * sizeof(H3_PartMetadata);
    H3_ObjectMetadata* objMeta = calloc(1, objMetaSize);
    memcpy(objMeta->userId, userId, sizeof(H3_UserId));
    objMeta->nParts = nParts;
    objMeta->lastModification = objMeta->lastAccess = objMeta->creation = time(NULL);

    //Upload part(s) if any
    if(data){
        int i;
        size_t roffset = offset;
        size_t rsize = size;
        KV_Value value = calloc(1, H3_PART_SIZE);

        for(i=0; i<nParts && storeStatus == KV_SUCCESS; i++){
            KV_Key key = GetKey(objId, i, -1);
            size_t partSize = min(roffset+rsize, H3_PART_SIZE);

            if(!roffset){
                value = &((KV_Value)data)[size - rsize];
                rsize -= min(H3_PART_SIZE, rsize);
            }
            else if(roffset > H3_PART_SIZE){
                roffset -= H3_PART_SIZE;
            }
            else { // roffset < H3_PART_SIZE
                size_t dataSize = min(H3_PART_SIZE-roffset, rsize);
                memcpy(&value[roffset], data, dataSize);
                roffset = 0;
                rsize -= dataSize;
            }

            storeStatus = uploadData(_handle, key, value, 0, partSize);
            objMeta->part[i].number = i;
            objMeta->part[i].size=partSize;
            free(key);
        }

        free(value);

        // Sanity check
        if(roffset || rsize){
            exit(-1);
        }
    }

    //Upload metadata
    if(storeStatus == KV_SUCCESS && uploadMetadata(_handle, objId, (KV_Value)objMeta, 0, objMetaSize) == KV_SUCCESS){
        status = H3_SUCCESS;
    }

    free(objMeta);

    return status;
}



int H3_CreateObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, size_t offset){
    return UploadObject(handle, token, bucketName, objectName, 1, data, size, offset);
}

int H3_WriteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, uint64_t offset, void* data, size_t size){

    // TODO - Check if we need to truncate the object

    return UploadObject(handle, token, bucketName, objectName, 0, data, size, offset);
}


// Object management
int H3_ListObjects(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, H3_Name* objectNames, size_t* size){return H3_FAILURE;}
int H3_ForeachObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}
int H3_InfoObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){return H3_FAILURE;}
int H3_ReadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, size_t maxSize, uint64_t offset, void* data, size_t* size){return H3_FAILURE;}
int H3_CopyObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_CopyObjectRange(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, uint64_t offset, size_t size, H3_Name dstObjectName){return H3_FAILURE;}
int H3_MoveObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){return H3_FAILURE;}
int H3_DeleteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName){return H3_FAILURE;}







//int H3_CreateObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, size_t offset){
//    int status = H3_FAILURE;
//
//    // Argument check. Note we allow zero-sized objects.
//    if(!handle || !token  || !bucketName || !objectName ){
//        return H3_FAILURE;
//    }
//
//    H3_Context* ctx = (H3_Context*)handle;
//    KV_Handle _handle = ctx->handle;
//    KV_Operations* op = ctx->operation;
//    KV_Status storeStatus = KV_SUCCESS;
//
//    H3_UserId userId;
//    H3_ObjectId objId;
//
//
//    // Validate bucketName & extract userId from token
//    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
//        return H3_FAILURE;
//    }
//
//    GetObjectId(bucketName, objectName, objId);
//
//    // Allocate & populate Object metadata
//    uint nParts = (offset + size + H3_PART_SIZE - 1)/H3_PART_SIZE;
//    size_t objMetaSize = sizeof(H3_ObjectMetadata) + nParts * sizeof(H3_PartMetadata);
//    H3_ObjectMetadata* objMeta = calloc(1, objMetaSize);
//    memcpy(objMeta->userId, userId, sizeof(H3_UserId));
//    objMeta->nParts = nParts;
//    objMeta->lastModification = objMeta->lastAccess = objMeta->creation = time(NULL);
//
//    //Upload part(s) if any
//    if(data){
//        int i;
//        size_t roffset = offset;
//        size_t rsize = size;
//        KV_Value value = calloc(1, H3_PART_SIZE);
//
//        for(i=0; i<nParts && storeStatus == KV_SUCCESS; i++){
//            KV_Key key = GetKey(objId, i, -1);
//            size_t partSize = min(roffset+rsize, H3_PART_SIZE);
//
//            if(!roffset){
//                value = &((KV_Value)data)[size - rsize];
//                rsize -= min(H3_PART_SIZE, rsize);
//            }
//            else if(roffset > H3_PART_SIZE){
//                roffset -= H3_PART_SIZE;
//            }
//            else { // roffset < H3_PART_SIZE
//                size_t dataSize = min(H3_PART_SIZE-roffset, rsize);
//                memcpy(&value[roffset], data, dataSize);
//                roffset = 0;
//                rsize -= dataSize;
//            }
//
//            storeStatus = op->create(_handle, key, value, 0, partSize);
//            objMeta->part[i].number = i;
//            objMeta->part[i].size=partSize;
//            free(key);
//        }
//
//        free(value);
//
//        // Sanity check
//        if(roffset || rsize){
//            exit(-1);
//        }
//    }
//
//    //Upload metadata
//    if(storeStatus == KV_SUCCESS && op->metadata_create(_handle, objId, objMeta, 0, objMetaSize) == KV_SUCCESS){
//        status = H3_SUCCESS;
//    }
//
//    free(objMeta);
//
//    return status;
//}







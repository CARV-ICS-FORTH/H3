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

//typedef KV_Status (*UploadData)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);
//typedef KV_Status (*UploadMetadata)(KV_Handle handle, KV_Key key, KV_Value value, uint64_t offset, size_t size);


int ValidateObjectName(char* name){
    regex_t regex;
    if( name && (strnlen(name, H3_OBJECT_NAME_SIZE) == H3_OBJECT_NAME_SIZE   ||          // Provided name is too big or
                 regcomp(&regex, "(^/)|([^/_0-9a-zA-Z-.])|(/{2,})", 0)       ||          // contains invalid characters or starts with "/" or has back-to-back "/"
                 regexec(&regex, name, 0, NULL, 0) == REG_NOERROR               ) ){

        return H3_FAILURE;
    }
    return H3_SUCCESS;
}

int H3_CreateObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, size_t offset){
    int status = H3_FAILURE;

    // Argument check. Note we allow zero-sized objects.
    if(!handle || !token  || !bucketName || !objectName ){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    // Make sure user has access to the bucket
    if(op->metadata_read(_handle, bucketName, 0, &value, &mSize) != KV_SUCCESS){
        return H3_FAILURE;
    }

    H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
    if(GrantBucketAccess(userId, bucketMetadata)){

        GetObjectId(bucketName, objectName, objId);

        // Allocate & populate Object metadata
        uint nParts = (offset + size + H3_PART_SIZE - 1)/H3_PART_SIZE;
        uint nBatch = (nParts + H3_PART_BATCH_SIZE - 1)/H3_PART_BATCH_SIZE;
        size_t objMetaSize = sizeof(H3_ObjectMetadata) + nBatch * H3_PART_BATCH_SIZE * sizeof(H3_PartMetadata);
        H3_ObjectMetadata* objMeta = calloc(1, objMetaSize);
        memcpy(objMeta->userId, userId, sizeof(H3_UserId));
        objMeta->nParts = nParts;
        objMeta->lastModification = objMeta->lastAccess = objMeta->creation = time(NULL);

        //Upload part(s) if any
        if(data){
            int i;
            size_t rsize = size;        // remaining size
            size_t roffset = offset;    // remaining offset
            KV_Value buffer = NULL;

            if(offset)
                buffer = calloc(1, H3_PART_SIZE);

            for(i=0, storeStatus = KV_SUCCESS; i<nParts && storeStatus == KV_SUCCESS; i++){
                size_t partSize = min(roffset+rsize, H3_PART_SIZE);

                if(!roffset){
                    value = &((KV_Value)data)[size - rsize];
                    rsize -= min(H3_PART_SIZE, rsize);
                }
                else if(roffset > H3_PART_SIZE){
                    value = buffer;
                    roffset -= H3_PART_SIZE;
                }
                else { // roffset < H3_PART_SIZE
                    size_t dataSize = min(H3_PART_SIZE-roffset, rsize);
                    memcpy(&buffer[roffset], data, dataSize);
                    roffset = 0;
                    rsize -= dataSize;
                }

                objMeta->part[i].number = i;
                objMeta->part[i].subNumber = -1;
                objMeta->part[i].size = partSize;
                CreatePartId(objMeta->part[i].id, objMeta->part[i].number, objMeta->part[i].subNumber);
                storeStatus = op->create(_handle, objMeta->part[i].id, value, 0, objMeta->part[i].size);
            }

            if(offset)
                free(buffer);

            if( storeStatus != KV_SUCCESS)
                objMeta->isBad = 1;

            // Sanity check
            if(roffset || rsize){
                exit(-1);
            }
        }

        //Upload object metadata
        if(op->metadata_create(_handle, objId, (KV_Value)objMeta, 0, objMetaSize) == KV_SUCCESS && !objMeta->isBad){
            status = H3_SUCCESS;
        }
        free(objMeta);
    }
    free(bucketMetadata);

    return status;
}

int H3_ReadObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, size_t offset, void* data, size_t* size){
    int status = H3_SUCCESS;

    // Argument check
    if(!handle || !token  || !bucketName || !objectName || !data || !size ){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t remaining = *size;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, objectName, objId);

    if( op->metadata_read(_handle, objId, 0, &value, &mSize) == KV_SUCCESS){
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;

        // Find initial part and in-part-offset for the user provided offset
        int i, initialPart = 0;
        size_t initialPartOffset = 0;
        size_t aggregateSize = 0;

        for(i=0; i<objMeta->nParts && (aggregateSize+=objMeta->part[i].size) < offset; i++);

        if(!GrantObjectAccess(userId, objMeta)  ||      // User has no access to object
            objMeta->isBad                      ||      // The object is corrupt
            i == objMeta->nParts                    ){  // Offset is out of range

            status = H3_FAILURE;

        }

        // Offset lays in some part other than the first
        else if(offset && i){
            aggregateSize -= objMeta->part[i].size;
            initialPartOffset = offset - aggregateSize;
            initialPart = --i;
        }

        // No offset or it lays within the first part
        else
            initialPartOffset = offset;

        if(status == H3_SUCCESS){

            // Retrieve data
            KV_Status storeStatus;
            value = (KV_Value)data;
            size_t partOffset = initialPartOffset;
             for(i=initialPart, storeStatus = KV_SUCCESS; i<objMeta->nParts && remaining && storeStatus == KV_SUCCESS; i++){
                size_t retrieved = remaining;
                storeStatus = op->read(_handle, objMeta->part[i].id, partOffset, &value, &retrieved);

                // Sanity check
                if(storeStatus == KV_SUCCESS && retrieved != min(objMeta->part[i].size - partOffset,remaining)){
                    exit(-1);
                }

                value = &value[retrieved];
                remaining -= retrieved;
                partOffset = 0;
            }

            // Update timestamp
            objMeta->lastAccess = time(NULL);
            op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize);
            *size = *size - remaining;
        }

        free(objMeta);
        return status;
    }
    return H3_FAILURE;
}

int H3_InfoObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){
    int status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName || !objectName || !objectInfo ){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, objectName, objId);

    if( op->metadata_read(_handle, objId, 0, &value, &mSize) == KV_SUCCESS){

        // Make sure user has access to the object
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        if(GrantObjectAccess(userId, objMeta)){

            int i;
            size_t size=0;
            for(i=0; i<objMeta->nParts; i++){
                size += objMeta->part[i].size;
            }

            objectInfo->name = strdup(objId);
            objectInfo->isBad = objMeta->isBad;
            objectInfo->lastAccess = objMeta->lastAccess;
            objectInfo->lastModification = objMeta->lastModification;
            objectInfo->size = size;

            objMeta->lastAccess = time(NULL);
            op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize);

            status = H3_SUCCESS;
        }
        free(objMeta);
    }

    return status;
}

int H3_DeleteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName){
    int status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName || !objectName){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, objectName, objId);

    if( op->metadata_read(_handle, objId, 0, &value, &mSize) == KV_SUCCESS ){

        // Make sure user has access to the object
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        if(GrantObjectAccess(userId, objMeta)){

            int i, nBadParts=0;
            H3_PartMetadata* badPart = malloc(sizeof(H3_PartMetadata) * objMeta->nParts);
            for(i=0; i<objMeta->nParts; i++){
                if(op->delete(_handle, objMeta->part[i].id) != KV_SUCCESS){
                    memcpy(&badPart[nBadParts++], &objMeta->part[i], sizeof(H3_PartMetadata));
                }
            }

            if(nBadParts){
                memcpy(objMeta->part, badPart, sizeof(H3_PartMetadata) * nBadParts);
                objMeta->isBad = 1;
                objMeta->nParts = nBadParts;
                objMeta->lastAccess = time(NULL);
                op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize);
                status = KV_FAILURE;
            }
            else if(op->metadata_delete(_handle, objId) == KV_SUCCESS){
                status = H3_SUCCESS;
            }
            free(badPart);
        }
        free(objMeta);
    }

    return status;
}



int H3_MoveObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){
    int status = H3_FAILURE;
    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(srcObjectName) || !ValidateObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    if( op->metadata_read(_handle, srcObjId, 0, &value, &mSize) == KV_SUCCESS){

        // Make sure the user has access to the object
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        if( GrantObjectAccess(userId, objMeta) ){

            if(op->metadata_create(_handle, dstObjId, (KV_Value)objMeta, 0, mSize) == KV_SUCCESS &&
                    op->metadata_delete(_handle, srcObjId) == KV_SUCCESS                                 ){
                status = H3_SUCCESS;
            }
        }
        free(objMeta);
    }

    return status;
}

int H3_CopyObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){
    int status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;

    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !ValidateObjectName(srcObjectName) || !ValidateObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    if( op->metadata_read(_handle, srcObjId, 0, &value, &mSize) == KV_SUCCESS){

        // Make sure the user has access to the object
        H3_ObjectMetadata* srcObjMeta = (H3_ObjectMetadata*)value;
        if( GrantObjectAccess(userId, srcObjMeta) ){
            int i;
            KV_Status storeStatus;
            H3_ObjectMetadata* dstObjMeta = malloc(mSize);
            memcpy(dstObjMeta, srcObjMeta, mSize);

            // Reserve the destination object
            dstObjMeta->isBad = 1;
            if(op->metadata_create(_handle, dstObjId, (KV_Value)dstObjMeta, 0, mSize) == KV_SUCCESS){

                // Copy the parts
                for(i=0, storeStatus = KV_SUCCESS; i<srcObjMeta->nParts && storeStatus == KV_SUCCESS; i++){
                    CreatePartId(dstObjMeta->part[i].id, dstObjMeta->part[i].number, dstObjMeta->part[i].subNumber);
                    storeStatus = op->copy(_handle, srcObjMeta->part[i].id, dstObjMeta->part[i].id);
                }

                // Update destination metadata
                dstObjMeta->lastAccess = dstObjMeta->lastModification = dstObjMeta->creation = time(NULL);
                if(storeStatus != KV_SUCCESS){
                    dstObjMeta->nParts = i;
                }
                else
                    dstObjMeta->isBad = 0;

                // Update source metadata
                srcObjMeta->lastAccess = time(NULL);

                if( op->metadata_write(_handle, dstObjId, (KV_Value)dstObjMeta, 0, mSize)== KV_SUCCESS &&
                    op->metadata_write(_handle, srcObjId, (KV_Value)srcObjMeta, 0, mSize)== KV_SUCCESS      ){
                    status =  H3_SUCCESS;
                }
            }

            free(dstObjMeta);
        }
        free(srcObjMeta);
    }

    return status;
}





// Object management
int H3_ListObjects(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t offset, H3_Name* objectNames, size_t* size){

//    // Argument check. Note a 'prefix' is not required.
//    if(!handle || !token  || !bucketName || !objectNames || !size){
//        return H3_FAILURE;
//    }
//
//    H3_Context* ctx = (H3_Context*)handle;
//    KV_Handle _handle = ctx->handle;
//    KV_Operations* op = ctx->operation;
//
//    H3_UserId userId;
//    H3_ObjectId objId;
//    KV_Value value = NULL;
//    KV_Key key;
//    uint64_t nKeys;
//    size_t mSize = 0;
//
//    // Validate bucketName & extract userId from token
//    if( !ValidateBucketName(bucketName) || !ValidateObjectName(prefix) || !GetUserId(token, userId) ){
//        return H3_FAILURE;
//    }
//
//    GetObjectId(bucketName, prefix, objId);
//    if(op->list(_handle, prefix, &key, &nKeys)){
//
//    }
//
////    op->


    return H3_FAILURE;
}




int H3_ForeachObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name prefix, size_t maxSize, uint64_t offset, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}


int H3_CopyObjectRange(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name srcObjectName, uint64_t offset, size_t size, H3_Name dstObjectName){return H3_FAILURE;}


int H3_WriteObject(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_Name objectName, uint64_t offset, void* data, size_t size){

    // TODO - Check if we need to truncate the object
    // TODO - reset the isBad flag

    return H3_FAILURE;
}

















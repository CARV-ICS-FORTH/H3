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


int ValidObjectName(char* name){
    int status = TRUE;

    if(name){

        // Too small/big
        regex_t regex;
        size_t nameSize = strnlen(name, H3_OBJECT_NAME_SIZE+1);
        if( nameSize == 0 || nameSize > H3_OBJECT_NAME_SIZE     ||
            regcomp(&regex, "(^/)|([^/_0-9a-zA-Z.-])|(/{2,})", 0) != REG_NOERROR    ){
            return FALSE;
        }

        // Contains invalid characters or starts with "/" or has back-to-back "/"
        if(regexec(&regex, name, 0, NULL, 0) != REG_NOMATCH){
            status = FALSE;
        }
        regfree(&regex);
    }

    return status;
}

int ValidPrefix(char* name){
    if(name){
        size_t nameSize = strnlen(name, H3_OBJECT_NAME_SIZE+1);
        if (nameSize == 0)
            return TRUE;
    }
    return ValidObjectName(name);
}

uint EstimateNumOfParts(H3_ObjectMetadata* objMeta, size_t size, off_t offset){
    int nParts = ((offset % H3_PART_SIZE) +  size + H3_PART_SIZE - 1)/H3_PART_SIZE;
    uint i, regionEnd = offset + size - 1;

    if(objMeta == NULL)
        return nParts;

    for(i=0; i<objMeta->nParts; i++){
        uint partEnd = objMeta->part[i].offset + objMeta->part[i].size - 1;

        // Add non-overlapping parts
        if(partEnd < offset || regionEnd < objMeta->part[i].offset){
            nParts++;
        }

        // Remove overlapping parts
        else
            nParts--;
    }

    return  max(objMeta->nParts, nParts);
}

int ComparePartMetadataByOffset(const void* partA, const void* partB) {
    return ((H3_PartMetadata*)partA)->offset - ((H3_PartMetadata*)partB)->offset;
}

KV_Status WriteData(H3_Context* ctx, H3_ObjectMetadata* meta, KV_Value value, size_t size, off_t offset, uint userPartNumber, H3_PartitionPolicy policy){
    /*
     * Used by H3_WriteObject, H3_WriteObjectCopy. If the object exists it is overwritten rather than truncated. Parts are of max-size
     * rather than fixed size, thus they can freely increase in size up to H3_PART_SIZE provided they do not overlap with the next part.
     *
     * Parts are left-padded in order the offset to be H3_PART_SIZE aligned. For ordinary objects the part offset dictates the part-number.
     * However in case of multipart-objects this may not be true since during completion they are sorted based on part their number/sub-number
     * and the offsets are adjusted such that the first part always starts at 0x00 followed by the others without gaps.
     *
     * On the other hand, ordinary objects can be sparse or a completed multipart-object that was later turned into sparse.
     *
     * Therefore, when updating an object we preserve the part number/sub-number/offset of any parts we overwrite taking care to set the new
     * new size such that it doesn't overlap with the next part (if any).
     *
     * Data segments for multipart objects are assigned an explicit part-number by the user thus they are broken into sub-parts.
     */

    KV_Status status = KV_SUCCESS;
    uint i, partNumber, partIndex, nNewParts = 0;
    int partSubNumber;
    off_t partOffset, inPartOffset;
    size_t partSize;

    meta->isBad = 0;
    while(status == KV_SUCCESS && size && !meta->isBad){

        // Initialize part arguments based on offset and size
        if(policy == DivideInParts ){
            partNumber = offset / H3_PART_SIZE;
            partSubNumber = -1;
            partOffset = partNumber * H3_PART_SIZE;
        }
        else {
            partNumber = userPartNumber;
            partSubNumber = offset / H3_PART_SIZE;
            partOffset = partSubNumber * H3_PART_SIZE;
        }

        inPartOffset = offset % H3_PART_SIZE;
        partSize = min((H3_PART_SIZE - inPartOffset), size);

        // Check for overlapping parts based on offset...
        char overlapping = 0;
        if(policy == DivideInParts ){
            uint regionEnd = offset + size -1;
            for(i=0; i<meta->nParts && !overlapping; i++){

                uint partEnd = meta->part[i].offset + meta->part[i].size - 1;

                if((meta->part[i].offset <= offset && offset <= partEnd)        ||      // Region starts within part
                   (meta->part[i].offset <= regionEnd && regionEnd <= partEnd)  ||      // Region ends within part
                   (offset <= meta->part[i].offset && partEnd <= regionEnd)         ){  // Region fully encompasses part

                    partNumber = meta->part[i].number;
                    partSubNumber = meta->part[i].subNumber;
                    partOffset = meta->part[i].offset;

                    // Check the next part for size restriction in case of multipart object
                    if( i < meta->nParts -1){
                        partSize = min(meta->part[i+1].offset - inPartOffset, partSize);
                    }

                    overlapping = 1;
                }
            }
        }
        else if(offset){ // i.e. DivideInSubParts
            for(i=0; i<meta->nParts && !overlapping; i++){
                uint partEnd = meta->part[i].offset + meta->part[i].size - 1;
                if(meta->part[i].number == userPartNumber &&
                   meta->part[i].offset <= offset && offset <= partEnd){
                    overlapping = 1;
                }
            }
        }

        if(overlapping){
            partIndex = i - 1; // Account for the auto-increment in the overlap detection loop
        }
        else {
            partIndex = meta->nParts + nNewParts;
            nNewParts++;
        }

        H3_PartId partId;
        CreatePartId(partId, meta->uuid, partNumber, partSubNumber);
        if( (status = ctx->operation->write(ctx->handle, partId, value, inPartOffset, partSize)) == KV_SUCCESS){
            // Create/Update metadata entry
            meta->part[partIndex].number = partNumber;
            meta->part[partIndex].subNumber = partSubNumber;
            meta->part[partIndex].offset = partOffset;
            meta->part[partIndex].size = inPartOffset + partSize;

            // Advance offset
            offset += partSize;
            value += partSize;
            size -= partSize;
        }
        else
            meta->isBad = 1;
    }

    // Update object metadata
    meta->nParts += nNewParts;
    meta->lastModification = meta->lastAccess  = time(NULL);
    qsort(meta->part, meta->nParts, sizeof(H3_PartMetadata), ComparePartMetadataByOffset);

    return status;
}

KV_Status ReadData(H3_Context* ctx, H3_ObjectMetadata* meta, KV_Value value, size_t* size, off_t offset){
    /*
     * 'value' is an allocated buffer big enough to fit 'size' bytes.
     */
    KV_Status status = KV_SUCCESS;

    // Identify starting part
    uint i;
    for(i=0; i<meta->nParts && offset > (meta->part[i].offset + meta->part[i].size - 1); i++);

    // Retrieve the data
    H3_PartId partId;
    size_t retrieved, remaining = *size;
    off_t partOffset = offset - meta->part[i].offset;

    for(; i<meta->nParts && remaining && status == KV_SUCCESS; i++){
        retrieved = remaining;
        CreatePartId(partId, meta->uuid, meta->part[i].number, meta->part[i].subNumber);
        status = ctx->operation->read(ctx->handle, partId, partOffset, &value, &retrieved);

        // TODO - Sanity check (SegFault)
        if(status == KV_SUCCESS && retrieved != min(meta->part[i].size - partOffset,remaining)){
            *((char*)0x00) = 1;
        }

        value = &value[retrieved];
        remaining -= retrieved;
        partOffset = 0;
    }

    *size -= remaining;

    return status;
}


KV_Status CopyData(H3_Context* ctx, H3_UserId userId, H3_ObjectId srcObjId, H3_ObjectId dstObjId, off_t srcOffset, size_t* size, uint8_t noOverwrite, off_t dstOffset){
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    KV_Status status = KV_FAILURE;
    KV_Value value = NULL;
    size_t mSize = 0;

    if( (status = op->metadata_read(_handle, srcObjId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure the user has access to the object
        H3_ObjectMetadata* srcObjMeta = (H3_ObjectMetadata*)value;
        if( GrantObjectAccess(userId, srcObjMeta) ){

            if((status = op->metadata_exists(_handle, dstObjId)) == KV_KEY_NOT_EXIST ||
               (status == KV_KEY_EXIST && !noOverwrite)                                 ) {

                // Reserve the destination object
                H3_ObjectMetadata* dstObjMeta = malloc(mSize);
                memcpy(dstObjMeta, srcObjMeta, mSize);
                uuid_generate(dstObjMeta->uuid);
                dstObjMeta->nParts = 0;
                if((status = op->metadata_create(_handle, dstObjId, (KV_Value)dstObjMeta, 0, mSize)) == KV_SUCCESS){

                    // Copy the data in parts
                    KV_Value buffer = malloc(H3_PART_SIZE);
                    size_t remaining = *size;

                    while(remaining && status == KV_SUCCESS){

                        size_t buffSize = min(H3_PART_SIZE, remaining);
                        if( (status = ReadData(ctx, srcObjMeta, buffer, &buffSize, srcOffset) == KV_SUCCESS)                    &&
                            (status = WriteData(ctx, dstObjMeta, buffer, buffSize, dstOffset, 0, DivideInParts) == KV_SUCCESS)     ){

                            remaining -= buffSize;
                            srcOffset += buffSize;
                            dstOffset += buffSize;
                        }
                    }// while()
                    *size -= remaining;

                    free(buffer);
                }
                free(dstObjMeta);
            }
            else
                status = KV_FAILURE;
        }
        free(srcObjMeta);
    }

    return status;
}



/*! \brief  Create an object
 *
 * Create an object associated with a specific user( derived from the token). The bucket must exist and the object should not.
 * The object name must not exceed a certain size and conform to the following rules:
 *  1. May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
 *  2. Must not start with a slash
 *  3. A slash must not be followed by another one
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[in]    data               Pointer to object data
 * @param[in]    size               Size of the object data
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Bucket does not exist or user has no access
 * @result \b H3_EXISTS             Object already exists
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_CreateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size){

    // Argument check. Note we allow zero-sized objects.
    if(!handle || !token  || !bucketName || !objectName ){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_BucketId bucketId;
    H3_ObjectId objId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    // Make sure user has access to the bucket
    if(op->metadata_read(_handle, bucketId, 0, &value, &mSize) != KV_SUCCESS){
        return H3_FAILURE;
    }

    // TODO - Check there is no multipart object with that name

    H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
    if(GrantBucketAccess(userId, bucketMetadata)){

        GetObjectId(bucketName, objectName, objId);

        // Allocate & populate Object metadata
        uint nParts = EstimateNumOfParts(NULL, size, 0);
        uint nBatch = (nParts + H3_PART_BATCH_SIZE - 1)/H3_PART_BATCH_SIZE;
        size_t objMetaSize = sizeof(H3_ObjectMetadata) + nBatch * H3_PART_BATCH_SIZE * sizeof(H3_PartMetadata);
        H3_ObjectMetadata* objMeta = calloc(1, objMetaSize);
        memcpy(objMeta->userId, userId, sizeof(H3_UserId));
        uuid_generate(objMeta->uuid);

        // Reserve object
        if( (kvStatus = op->metadata_create(_handle, objId, (KV_Value)objMeta, 0, objMetaSize)) == KV_SUCCESS){

            // Write object
//            objMeta->nParts = nParts;
            objMeta->creation = time(NULL);
            objMeta->isBad = WriteData(ctx, objMeta, data, size, 0, 0, DivideInParts) != KV_SUCCESS?1:0;
            if( op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, objMetaSize) == KV_SUCCESS && !objMeta->isBad){
                status = H3_SUCCESS;
            }
        }
        else if(kvStatus == KV_KEY_EXIST)
            status = H3_EXISTS;

        free(objMeta);
    }
    free(bucketMetadata);

    return status;
}




/*! \brief  Retrieve data from an object
 *
 * Retrieve an object's content staring at offset. If both size and data arguments are 0x00 and NULL respectively then a buffer
 * is allocated internally to fit the entire object. In this case it is the responsibility of the user to free the buffer.
 * If a size is provided it is expected that a valid buffer of appropriate size has also been provided.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[in]    offset             Offset within the object's data
 * @param[in]    data               Pointer to hold the data
 * @param[inout] size               Size of data to retrieve or have been retrieved
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_CONTINUE           Operation completed successfully, though more data are available
 * @result \b H3_FAILURE            Bucket does not exist or user has no access or unable to allocate a buffer to fit the entire object
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_ReadObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, off_t offset, void** data, size_t* size){

    // Argument check
    if(!handle || !token  || !bucketName || !objectName || !data || !size ){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, objectName, objId);
    if( (storeStatus = op->metadata_read(_handle, objId, 0, &value, &mSize)) == KV_SUCCESS){
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        size_t objectSize = 0;

        if(objMeta->nParts)
            objectSize = objMeta->part[objMeta->nParts-1].offset + objMeta->part[objMeta->nParts-1].size;

        // User has access, the object is healthy and the offset is reasonable
        if(GrantObjectAccess(userId, objMeta) && !objMeta->isBad && offset < objectSize){

        	// NOTE Although the back-end is able to allocate the memory to retrieve the value of a KV pair
        	// (i.e. a part) here we are concerned with the OBJECT size thus we allocate a buffer (if needed)
        	// to hold several parts.
            uint freeOnFail = 0;
            if(*size == 0 && *data == NULL){
                *size = min(objectSize - offset, H3_CHUNK);
                *data = malloc(*size);
                freeOnFail = 1;
            }

            objMeta->lastAccess = time(NULL);
            if(*data){
                if( ReadData(ctx, objMeta, *data, size, offset) == KV_SUCCESS                      &&
                    op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize) == KV_SUCCESS     ){

                	if((objectSize - offset) > *size)
                		status = H3_CONTINUE;
                	else
                		status = H3_SUCCESS;
                }
                else if(freeOnFail)
                    free(*data);

            }
        }

        free(objMeta);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}




/*! \brief  Retrieve information about an object
 *
 * Retrieve an object's size, health status and creation, etc timestamps.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[in]    objectInfo         Pointer to a data-structure to be filled with information

 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to retrieve object info or user has no access
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_InfoObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo){

    // Argument check
    if(!handle || !token  || !bucketName || !objectName || !objectInfo ){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, objectName, objId);
    if((storeStatus = op->metadata_read(_handle, objId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure user has access to the object
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        if(GrantObjectAccess(userId, objMeta)){

            objectInfo->isBad = objMeta->isBad;
            objectInfo->lastAccess = objMeta->lastAccess;
            objectInfo->lastModification = objMeta->lastModification;

            if(objMeta->nParts)
                objectInfo->size = objMeta->part[objMeta->nParts-1].offset + objMeta->part[objMeta->nParts-1].size;
            else
                objectInfo->size = 0;

            status = H3_SUCCESS;
        }
        free(objMeta);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}

H3_Status DeleteObject(H3_Context* ctx, H3_UserId userId, H3_ObjectId objId, char truncate){
    H3_Status status = H3_FAILURE;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    if( (storeStatus = op->metadata_read(_handle, objId, 0, &value, &mSize)) == KV_SUCCESS ){

        // Make sure user has access to the object
        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
        if(GrantObjectAccess(userId, objMeta)){

            uint i;
            H3_PartId partId;
            for(i = objMeta->nParts, storeStatus = KV_SUCCESS; i-- >0 && storeStatus == KV_SUCCESS; objMeta->nParts--){
                storeStatus = op->delete(_handle, PartToId(partId, objMeta->uuid, &objMeta->part[i]));
            }

            objMeta->lastAccess = time(NULL);
            if(storeStatus != KV_SUCCESS){
                objMeta->isBad = 1;
                op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize);
            }
            else if(( truncate && op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, mSize) == KV_SUCCESS) ||
            		(!truncate && op->metadata_delete(_handle, objId) == KV_SUCCESS)								){
                status = H3_SUCCESS;
            }
        }
        free(objMeta);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}



/*! \brief  Delete an object
 *
 * Permanently deletes an object.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to retrieve object info or user has no access
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_DeleteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName){

    // Argument check
    if(!handle || !token  || !bucketName || !objectName){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;

    H3_UserId userId;
    H3_ObjectId objId;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, objectName, objId);

    return DeleteObject(ctx, userId, objId, 0);
}

/*! \brief  Truncate an object
 *
 * Permanently deletes an object.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[in]    size        		Size of truncated object. If the file previously was larger than this size, the extra data is lost.
 * 									If the file previously was shorter, it is extended, and the extended part reads as null bytes ('\0').
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to retrieve object info or user has no access
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments or size != 0x00
 *
 */
H3_Status H3_TruncateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, size_t size){
    // Argument check
    if(!handle || !token  || !bucketName || !objectName || size){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;

    H3_UserId userId;
    H3_ObjectId objId;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, objectName, objId);

    return DeleteObject(ctx, userId, objId, 1);
}


H3_Status MoveObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, H3_MovePolicy policy){

    LogActivity(H3_DEBUG_MSG, "Enter\n");

    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t srcMetaSize = 0;
    size_t dstMetaSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(srcObjectName) || !ValidObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    if( (storeStatus = op->metadata_read(_handle, srcObjId, 0, &value, &srcMetaSize)) == KV_SUCCESS){

        // Make sure the user has access to the source object
        H3_ObjectMetadata* srcObjMeta = (H3_ObjectMetadata*)value;
        if( GrantObjectAccess(userId, srcObjMeta) ){

        	value = NULL;
        	switch(op->metadata_read(_handle, dstObjId, 0, &value, &dstMetaSize)){
        		case KV_SUCCESS:{
        			// Make sure the user has access to the destination object
        			H3_ObjectMetadata* dstObjMeta = (H3_ObjectMetadata*)value;
        			if( GrantObjectAccess(userId, dstObjMeta) ){
        				switch(policy){
							case MoveReplace:
								if( DeleteObject(ctx, userId, dstObjId, 0) == H3_SUCCESS        	&&
								    op->metadata_move(_handle, srcObjId, dstObjId) == KV_SUCCESS 		){
									status = H3_SUCCESS;
								}
								break;

							case MoveNoReplace:
								status = H3_EXISTS;
								break;

							case MoveExchange:
								if(op->metadata_write(_handle, srcObjId, (KV_Value)dstObjMeta, 0, dstMetaSize) == KV_SUCCESS &&
								   op->metadata_write(_handle, dstObjId, (KV_Value)srcObjMeta, 0, srcMetaSize) == KV_SUCCESS	){
									status = H3_SUCCESS;
								}
								break;
        				}
        			}
        			free(dstObjMeta);
        		}
        		break;

        		case KV_KEY_NOT_EXIST:
        			if(policy != MoveExchange && op->metadata_move(_handle, srcObjId, dstObjId) == KV_SUCCESS){ status = H3_SUCCESS; }
        			break;

        		default:
        			status = H3_FAILURE;
        			break;
        	}
        }
        free(srcObjMeta);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    LogActivity(H3_DEBUG_MSG, "Exit - %d\n", status );
    return status;
}


/*! \brief  Rename an object
 *
 * Renames and object provided the new name is not taken by another object unless it is explicitly allowed
 * by the user in which case the previous object will be overwritten. Note that both objects will rest within
 * the same bucket.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    srcObjectName      The name of the object to be renamed
 * @param[in]    dstObjectName      The new name to be assumed by the object
 * @param[in]    noOverwrite        Indicates whether replacing is permitted
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object or user has no access
 * @result \b H3_NOT_EXISTS         Source object does not exist
 * @result \b H3_EXISTS         	Destination object exists and we are not allowed to replace it
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_MoveObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, uint8_t noOverwrite){
	return MoveObject(handle, token, bucketName, srcObjectName, dstObjectName, noOverwrite?MoveNoReplace:MoveReplace);
}

/*! \brief  Swap two objects
 *
 * Renames and object provided the new name is not taken by another object unless it is explicitly allowed
 * by the user in which case the previous object will be overwritten. Note that both objects will rest within
 * the same bucket.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    srcObjectName      The name of the object to be renamed
 * @param[in]    dstObjectName      The new name to be assumed by the object
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object or user has no access or destination object does not exist
 * @result \b H3_NOT_EXISTS         Source object does not exist
 * @result \b H3_EXISTS         	Destination object exists and we are not allowed to replace it
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_ExchangeObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName){
	return MoveObject(handle, token, bucketName, srcObjectName, dstObjectName, MoveExchange);
}



/*! \brief  Copy an object
 *
 * Copies and object provided the new name is not taken by another object unless it is explicitly allowed
 * by the user in which case the previous object will be overwritten. Note that both objects will rest within
 * the same bucket.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    srcObjectName      The name of the object to be renamed
 * @param[in]    dstObjectName      The new name to be assumed by the object
 * @param[in]    noOverwrite        Overwrite flag.
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object or user has no access or new name is in use and not allowed to overwrite
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_CopyObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, uint8_t noOverwrite){

    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(srcObjectName) || !ValidObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    if( (storeStatus = op->metadata_read(_handle, srcObjId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure the user has access to the object
        H3_ObjectMetadata* srcObjMeta = (H3_ObjectMetadata*)value;
        if( GrantObjectAccess(userId, srcObjMeta) ){

            // TODO - Instead of exist do a create

            if(( (storeStatus = op->metadata_exists(_handle, dstObjId)) == KV_FAILURE) ||
                 (storeStatus == KV_KEY_EXIST &&  (noOverwrite || DeleteObject(ctx, userId, dstObjId, 0) == H3_FAILURE))  ){
                status = H3_FAILURE;
            }
            else {
                int i;
                H3_ObjectMetadata* dstObjMeta = malloc(mSize);
                memcpy(dstObjMeta, srcObjMeta, mSize);

                // Reserve the destination object
                uuid_generate(dstObjMeta->uuid);
                dstObjMeta->nParts = 0;
                if(op->metadata_create(_handle, dstObjId, (KV_Value)dstObjMeta, 0, mSize) == KV_SUCCESS){

                    // Copy the parts
                    H3_PartId srcPartId, dstPartId;
                    for(i=0, storeStatus = KV_SUCCESS; i<srcObjMeta->nParts && storeStatus == KV_SUCCESS; i++){
                        PartToId(srcPartId, srcObjMeta->uuid, &srcObjMeta->part[i]);
                        CreatePartId(dstPartId, dstObjMeta->uuid, dstObjMeta->part[i].number, dstObjMeta->part[i].subNumber);
                        storeStatus = op->copy(_handle, srcPartId, dstPartId);
                    }

                    // Update destination metadata
                    dstObjMeta->lastAccess = dstObjMeta->lastModification = dstObjMeta->creation = time(NULL);
                    dstObjMeta->nParts = i;
                    if(storeStatus != KV_SUCCESS){
                        dstObjMeta->isBad = 1;
                    }

                    // Update source metadata
                    srcObjMeta->lastAccess = time(NULL);

                    if( op->metadata_write(_handle, dstObjId, (KV_Value)dstObjMeta, 0, mSize)== KV_SUCCESS &&
                        op->metadata_write(_handle, srcObjId, (KV_Value)srcObjMeta, 0, mSize)== KV_SUCCESS      ){
                        status =  H3_SUCCESS;
                    }
                }

                free(dstObjMeta);
            }
        }
        free(srcObjMeta);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}




/*! \brief  Retrieve objects matching a pattern
 *
 * Produce a list of object names with object matching a given pattern. The pattern is a simple prefix
 * rather than a regular expression. The pattern must adhere to the object naming conventions.
 * Upon success the buffer will contain a number of variable sized C strings (stored back to back) thus
 * it is the responsibility of the user to dispose it. In case the internal buffer is not big enough to
 * fit all matching entries (indicated by the operation status) the user may invoke again the function
 * with an appropriately set offset in order to retrieve the next batch of names.
 * In case of an error, the buffer will not be created.
 *
 * @param[in]     handle             An h3lib handle
 * @param[in]     token              Authentication information
 * @param[in]     bucketName         The name of the bucket to host the object
 * @param[in]     prefix             The initial part of an object name
 * @param[in]     offset             The number of matching names to skip
 * @param[out]    objectNameArray    Pointer to a C string buffer
 * @param[inout]  nObjects           Number of names in buffer
 *
 * @result \b H3_SUCCESS            Operation completed successfully (no more matching names exist)
 * @result \b H3_CONTINUE           Operation completed successfully (there could be more matching names)
 * @result \b H3_FAILURE            Unable to access bucket or user has no access
 * @result \b H3_NOT_EXISTS         Bucket does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_ListObjects(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t offset, H3_Name* objectNameArray, uint32_t* nObjects){

    // Argument check. Note a 'prefix' is not required.
    if(!handle || !token  || !bucketName || !objectNameArray || !nObjects){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidPrefix(prefix) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    if( (storeStatus = op->metadata_read(_handle, bucketId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure the token grants access to the bucket
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        if( GrantBucketAccess(userId, bucketMetadata) ){

            KV_Key keyBuffer = calloc(1, KV_LIST_BUFFER_SIZE);
            if(keyBuffer){

                H3_ObjectId objId;
                GetObjectId(bucketName, prefix, objId);
                uint8_t trim = strlen(bucketName) + 1; // Remove the bucketName prefix from the matching entries
                if( (storeStatus = op->list(_handle, objId, trim, keyBuffer, offset, nObjects)) != KV_FAILURE){
                    *objectNameArray = keyBuffer;
                    status = storeStatus==KV_SUCCESS?H3_SUCCESS:H3_CONTINUE;
                }
                else
                    free(keyBuffer);
            }
        }
        free(bucketMetadata);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}


/*! \brief  Execute a user provide function for each matching object
 *
 * Execute a user provide function for each object in a bucket matching a prefix. At each invocation the function
 * is passed the object's name along with the supplied user data.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    prefix             The initial part of an object name
 * @param[in]    offset             The number of matching objects to skip
 * @param[in]    function           The user function
 * @param[in]    userData           The user data
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object info or user has no access
 * @result \b H3_NOT_EXISTS         Bucket does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_ForeachObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t nObjects, uint32_t offset, h3_name_iterator_cb function, void* userData){

    // Argument check. Note a 'prefix' is not required.
    if(!handle || !token  || !bucketName){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Status storeStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidPrefix(prefix) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    if( (storeStatus = op->metadata_read(_handle, bucketId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure the token grants access to the bucket
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        if( GrantBucketAccess(userId, bucketMetadata) ){
            H3_ObjectId objId;
            KV_Key keyBuffer = calloc(1, KV_LIST_BUFFER_SIZE);
            uint32_t nKeys = 0;

            GetObjectId(bucketName, prefix, objId);
            uint8_t trim = strlen(bucketName) + 1; // Remove the bucketName prefix from the matching entries
            while((storeStatus = op->list(_handle, objId, trim, keyBuffer, offset, &nKeys)) == KV_CONTINUE || storeStatus == KV_SUCCESS){

                if(!nKeys) break;
                offset += nKeys;

                char* object = keyBuffer;
                while(nKeys--){
                    function(object, userData);
                    object += strlen(object) + 1;
                }
            }

            free(keyBuffer);
            if(storeStatus == KV_SUCCESS)
                status = H3_SUCCESS;
        }
        free(bucketMetadata);
    }
    else if(storeStatus == KV_KEY_NOT_EXIST)
        status = H3_NOT_EXISTS;

    return status;
}



/*! \brief  Write an object
 *
 * Write an object associated with a specific user( derived from the token). If the object exists it
 * will be overwritten. Sparse objects are supported.
 * The object name must not exceed a certain size and conform to the following rules:
 *  1. May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
 *  2. Must not start with a slash
 *  3. A slash must not be followed by another one
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[in]    data               Pointer to object data
 * @param[in]    size               Size of the object data
 * @param[in]    offset             Offset from the object's 0x00 byte
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Bucket does not exist or user has no access
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_WriteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, off_t offset){

    LogActivity(H3_DEBUG_MSG, "Enter\n");

    // Argument check. Note we allow zero-sized objects.
    if(!handle || !token  || !bucketName || !objectName || (size && !data) ){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_ObjectId objId;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(objectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    // TODO - Check the object is not a pending multipart-upload

    GetObjectId(bucketName, objectName, objId);
    if(op->metadata_exists(_handle, objId) == KV_KEY_NOT_EXIST){
       return H3_CreateObject(handle, token, bucketName, objectName, data, size);
    }

    // Get object metadata and make sure we have access
    if(op->metadata_read(_handle, objId, 0, &value, &mSize) != KV_SUCCESS){
        return H3_FAILURE;
    }

    H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
    if(GrantObjectAccess(userId, objMeta)){

        // Expand object metadata if needed
        uint nParts = EstimateNumOfParts(objMeta, size, offset);
        uint nBatch = (nParts + H3_PART_BATCH_SIZE - 1)/H3_PART_BATCH_SIZE;
        size_t objMetaSize = sizeof(H3_ObjectMetadata) + nBatch * H3_PART_BATCH_SIZE * sizeof(H3_PartMetadata);
        if(objMetaSize > mSize)
            objMeta = realloc(objMeta, objMetaSize);

#ifndef DEBUG
        if( WriteData(ctx, objMeta, data, size, offset, 0, DivideInParts) == KV_SUCCESS         &&
            op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, objMetaSize) == KV_SUCCESS     ){
            status = H3_SUCCESS;
        }
#else
        if( WriteData(ctx, objMeta, data, size, offset, 0, DivideInParts) != KV_SUCCESS ){
            LogActivity(H3_ERROR_MSG, "failed to write data\n");
        }
        else if(op->metadata_write(_handle, objId, (KV_Value)objMeta, 0, objMetaSize) != KV_SUCCESS){
            LogActivity(H3_ERROR_MSG, "failed to update meta-data\n");
        }
        else
            status = H3_SUCCESS;
#endif
    }
#ifdef DEBUG
    else
        LogActivity(H3_DEBUG_MSG, "failed to grant access userid:%s Vs metadata:%s\n", userId, objMeta->userId);
#endif

    free(objMeta);

    LogActivity(H3_DEBUG_MSG, "Exit - %d\n", status );
    return status;
}


/*! \brief  Copy a part of an object into a new one.
 *
 * Copies a segment of an object into another object provided the new name is not taken. Note that both objects will rest within
 * the same bucket.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    srcObjectName      The name of the object to be renamed
 * @param[in]    dstObjectName      The new name to be assumed by the object
 * @param[in]    offset             Offset with respect to the source object's 0x00 byte
 * @param[inout] size               The amount of data to copy
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object or user has no access or new name is in use
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_CreateObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t offset, size_t* size, H3_Name dstObjectName){

    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_INVALID_ARGS;
    }

    H3_Status status;
    H3_Context* ctx = (H3_Context*)handle;
    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(srcObjectName) || !ValidObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    switch(CopyData(ctx, userId, srcObjId, dstObjId, offset, size, 1, 0)){
        case KV_SUCCESS:
            status = H3_SUCCESS;
            break;

        case KV_KEY_NOT_EXIST:
            status = H3_NOT_EXISTS;
            break;

        default:
            status = H3_FAILURE;
    }

    return status;
}




/*! \brief  Copy a part of an object into a new or existing one.
 *
 * Copies a segment of an object into another object at a user provided offset. Note that both objects will rest within
 * the same bucket.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    srcObjectName      The name of the object to be renamed
 * @param[in]    dstObjectName      The new name to be assumed by the object
 * @param[in]    srcOffset          Offset with respect to the source object's 0x00 byte
 * @param[in]    dstOffset          Offset with respect to the destination object's 0x00 byte
 * @param[inout] size               The amount of data to copy
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Unable to access object or user has no access
 * @result \b H3_NOT_EXISTS         Object does not exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_WriteObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t srcOffset, size_t* size, H3_Name dstObjectName, off_t dstOffset){

    // Argument check
    if(!handle || !token  || !bucketName || !srcObjectName || !dstObjectName){
        return H3_INVALID_ARGS;
    }

    H3_Status status;
    H3_Context* ctx = (H3_Context*)handle;
    H3_UserId userId;
    H3_ObjectId srcObjId, dstObjId;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !ValidObjectName(srcObjectName) || !ValidObjectName(dstObjectName) || !GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    GetObjectId(bucketName, srcObjectName, srcObjId);
    GetObjectId(bucketName, dstObjectName, dstObjId);

    switch(CopyData(ctx, userId, srcObjId, dstObjId, srcOffset, size, 0, dstOffset)){
        case KV_SUCCESS:
            status = H3_SUCCESS;
            break;

        case KV_KEY_NOT_EXIST:
            status = H3_NOT_EXISTS;
            break;

        default:
            status = H3_FAILURE;
    }

    return status;
}


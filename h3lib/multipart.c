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

#include <glib.h>
#include <glib/gi18n.h>

#include "common.h"
#include "util.h"


int ComparePartMetadataByNumber(const void* a, const void* b) {
   H3_PartMetadata* partA = (H3_PartMetadata*) a;
   H3_PartMetadata* partB = (H3_PartMetadata*) b;

   if(partA->number == partB->number)
       return partA->subNumber - partB->subNumber;

   return partA->number - partB->number;
}




/*! \brief  Initiate a multipart object
 *
 * Initiate a multipart object associated with a specific user( derived from the token). Though the bucket must exist
 * there are no checks for the object itself since they are performed during completion. Once initiated the object
 * is accessed  through its ID rather than its name.
 * The object name must not exceed a certain size and conform to the following rules:
 *  1. May only consist of characters 0-9, a-z, A-Z, _ (underscore). / (slash) , - (minus) and . (dot).
 *  2. Must not start with a slash
 *  3. A slash must not be followed by another one
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to host the object
 * @param[in]    objectName         The name of the object to be created
 * @param[out]   multipartId        The id corresponding to the object
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_FAILURE            Bucket does not exist or user has no access
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_NAME_TOO_LONG      Bucket or Object name is longer than H3_BUCKET_NAME_SIZE or H3_OBJECT_NAME_SIZE respectively
 *
 */
H3_Status H3_CreateMultipart(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* multipartId){
    /*
     * Multipart objects consist of a temporary object and an indirector. The temporary object is identical to an ordinary one,
     * though it follows different naming conventions so it will not involuntarily be affected by ordinary object operations.
     * The user may manipulate the multipart object through the indirector which is identified by the multipart-id.
     */

    // Argument check.
    if(!handle || !token  || !bucketName || !objectName ){
        return H3_INVALID_ARGS;
    }

    H3_Status status;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    size_t mSize = 0;
    KV_Status storeStatus;

    // Validate bucketName & extract userId from token
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS || (status = ValidObjectName(op, objectName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    // Make sure user has access to the bucket
    if((storeStatus = op->metadata_read(_handle, bucketId, 0, &value, &mSize)) == KV_KEY_TOO_LONG){
        return H3_NAME_TOO_LONG;
    }
    else if(storeStatus != KV_SUCCESS)
        return H3_FAILURE;

    status = H3_FAILURE;
    H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
    if(GrantBucketAccess(userId, bucketMetadata)){

        // Populate temporary object metadata
        H3_ObjectMetadata objMeta;
        memcpy(objMeta.userId, userId, sizeof(H3_UserId));
        uuid_generate(objMeta.uuid);
        objMeta.isBad = 0;

        // Populate multipart metadata
        H3_MultipartMetadata multiMeta;
        GetMultipartObjectId(bucketName, objectName, multiMeta.objectId);
        memcpy(multiMeta.userId, userId, sizeof(H3_UserId));

        // Generate multipart ID
        *multipartId = GenerateMultipartId(objMeta.uuid);

        objMeta.nParts = 0;
        clock_gettime(CLOCK_REALTIME, &objMeta.creation);

        // Upload multipart and temp object metadata
        if((storeStatus = op->metadata_create(_handle, multiMeta.objectId, (KV_Value)&objMeta, sizeof(H3_ObjectMetadata))) == KV_SUCCESS){
            if( (storeStatus = op->metadata_create(_handle, *multipartId, (KV_Value)&multiMeta, sizeof(H3_MultipartMetadata))) == KV_SUCCESS){
                status = H3_SUCCESS;
            }
            else
                op->metadata_delete(_handle, multiMeta.objectId);
        }

        if(storeStatus == KV_KEY_TOO_LONG)
            status = H3_NAME_TOO_LONG;
    }

    free(bucketMetadata);

    return status;
}



/*! \brief  Finalize a multipart object
 *
 * Converts a multipart object into an ordinary one by coalescing uploaded parts ordered by their part-ID.
 * If another object with that name already exists it is overwritten. Once completed, the multipart-ID is
 * invalidated thus the multipart API becomes in-applicable for the object.
 *
 * @param[in]   handle              An h3lib handle
 * @param[in]   token               Authentication information
 * @param[in]   multipartId         The object id
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Multipart object does not exist
 * @result \b H3_FAILURE            User has no access or unable to access multipart object or no parts have been uploaded
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_CompleteMultipart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId){

    // Argument check.
    if(!handle || !token  || !multipartId){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    H3_UserId userId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Extract userId from token...
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    // ...and retrieve multipart metadata
    if((kvStatus = op->metadata_read(_handle, multipartId, 0, &value, &mSize)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_FAILURE )
        return H3_FAILURE;

    // Make sure user has access to the multipart-object
    H3_MultipartMetadata* multiMeta = (H3_MultipartMetadata*)value;
    if(GrantMultipartAccess(userId, multiMeta)){
        value = NULL; mSize = 0;
        if(op->metadata_read(_handle, multiMeta->objectId, 0, &value, &mSize) == KV_SUCCESS){
            H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
            if(objMeta->nParts){

                // Sort temporary object metadata and set offsets
                uint i;
                off_t offset = 0;
                qsort(objMeta->part, objMeta->nParts, sizeof(H3_PartMetadata), ComparePartMetadataByNumber);
                for(i=0; i<objMeta->nParts; i++){
                    objMeta->part[i].offset = offset;
                    offset += objMeta->part[i].size;
                }


                // Convert temporary ID to ordinary
                H3_ObjectId objId;
                memcpy(objId, multiMeta->objectId, sizeof(H3_ObjectId));
                ConvertToOdrinary(objId);


                // Create ordinary object (delete pre-existing ordinary object with same ID if any)
                if( (kvStatus = op->metadata_create(_handle, objId, (KV_Value)objMeta, mSize)) == KV_SUCCESS  ||
                    (kvStatus == KV_KEY_EXIST && DeleteObject(ctx, userId, objId, 0) == H3_SUCCESS &&
                     op->metadata_create(_handle, objId, (KV_Value)objMeta, mSize) == KV_SUCCESS               )   ){

                    // Delete temporary object metadata and indirector
                    if( op->metadata_delete(_handle, multiMeta->objectId)== KV_SUCCESS &&
                        op->metadata_delete(_handle, multipartId)== KV_SUCCESS              ){
                        status = H3_SUCCESS;
                    }
                }
            }
            free(objMeta);
        }
    }
    free(multiMeta);
    return status;
}



/*! \brief  Delete a multipart object
 *
 * Deletes a multipart object along with all uploaded parts if any. Once deleted, the multipart-ID is
 * invalidated.
 *
 * @param[in]   handle             An h3lib handle
 * @param[in]   token              Authentication information
 * @param[in]   multipartId        The object id
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Multipart object does not exist
 * @result \b H3_FAILURE            User has no access or unable to access multipart object
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_AbortMultipart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId){

    // Argument check.
    if(!handle || !token  || !multipartId){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    H3_UserId userId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Extract userId from token...
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    // ...and retrieve multipart metadata
    if((kvStatus = op->metadata_read(_handle, multipartId, 0, &value, &mSize)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_FAILURE )
        return H3_FAILURE;

    // Make sure user has access to the multipart-object
    H3_MultipartMetadata* multiMeta = (H3_MultipartMetadata*)value;
    if( GrantMultipartAccess(userId, multiMeta)                            &&
        DeleteObject(ctx, userId, multiMeta->objectId, 0) == H3_SUCCESS    &&
        op->metadata_delete(_handle, multipartId) == KV_SUCCESS               ){
        status = H3_SUCCESS;
    }

    free(multiMeta);

    return status;
}



/*! \brief  Get list of multipart objects
 *
 * Retrieve the ID of all multipart objects in a bucket into an internally allocated array.
 * Note it is the responsibility of the user to dispose the array except in case of error.
 *
 * @param[in]     handle             An h3lib handle
 * @param[in]     token              Authentication information
 * @param[in]     multipartId        The object id
 * @param[in]     offset             The number of IDs to skip
 * @param[out]    multipartIdArray   An array of IDs
 * @param[inout]  nIds               The number of IDs
 *
 * @result \b H3_SUCCESS            Operation completed successfully (no more IDs exist)
 * @result \b H3_CONTINUE           Operation completed successfully (there could be more IDs)
 * @result \b H3_NOT_EXISTS         Bucket does not exist
 * @result \b H3_FAILURE            User has no access or unable to access bucket
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_ListMultiparts(H3_Handle handle, H3_Token token, H3_Name bucketName, uint32_t offset, H3_MultipartId* multipartIdArray, uint32_t* nIds){

    // Argument check. Note a 'prefix' is not required.
    if(!handle || !token  || !bucketName || !multipartIdArray || !nIds){
        return H3_INVALID_ARGS;
    }

    H3_Status status;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Validate bucketName & extract userId from token
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    status = H3_FAILURE;
    if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &mSize)) == KV_SUCCESS){

        // Make sure the token grants access to the bucket
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        if( GrantBucketAccess(userId, bucketMetadata) ){

            KV_Key keyBuffer = calloc(1, KV_LIST_BUFFER_SIZE);
            if(keyBuffer){

                H3_ObjectId objId;
                GetMultipartObjectId(bucketName, NULL, objId);
                uint8_t trim = strlen(bucketName) + 1; // Remove the bucketName prefix from the matching entries
                if( (kvStatus = op->list(_handle, objId, trim, keyBuffer, offset, nIds)) != KV_FAILURE){
                    *multipartIdArray = keyBuffer;
                    status = kvStatus==KV_SUCCESS?H3_SUCCESS:H3_CONTINUE;
                }
                else
                    free(keyBuffer);
            }
        }
        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST)
        return H3_NOT_EXISTS;
    else if(kvStatus == KV_KEY_TOO_LONG)
        return H3_NAME_TOO_LONG;

    return status;
}



/*! \brief  Get part-list of a multipart object
 *
 * Retrieves information for each part of a multipart object into an internally allocated array.
 * Note it is the responsibility of the user to dispose the array except in case of error.
 *
 * @param[in]   handle             An h3lib handle
 * @param[in]   token              Authentication information
 * @param[in]   multipartId        The object id
 * @param[out]  partInfoArray      An array of parts
 * @param[out]  nParts             The number of parts
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Multipart object does not exist
 * @result \b H3_FAILURE            User has no access or unable to access multipart object
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_ListParts(H3_Handle handle, H3_Token token, H3_MultipartId multipartId, H3_PartInfo** partInfoArray, uint32_t* nParts){

    // Argument check.
    if(!handle || !token  || !multipartId || !partInfoArray || !nParts){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    H3_UserId userId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Extract userId from token...
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    // ...and retrieve multipart metadata
    if((kvStatus = op->metadata_read(_handle, multipartId, 0, &value, &mSize)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_FAILURE )
        return H3_FAILURE;

    // Make sure user has access to the multipart-object
    H3_MultipartMetadata* multiMeta = (H3_MultipartMetadata*)value;
    if(GrantMultipartAccess(userId, multiMeta)){
        value = NULL; mSize = 0;
        if(op->metadata_read(_handle, multiMeta->objectId, 0, &value, &mSize) == KV_SUCCESS){

            // Create hash table on partNumber with size as value
            H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
            uint i;
            GHashTable* partTable = g_hash_table_new(NULL, NULL);
            for(i=0; i<objMeta->nParts; i++){
                size_t size = GPOINTER_TO_UINT(g_hash_table_lookup(partTable, GUINT_TO_POINTER(objMeta->part[i].number)));
                if(size){
                    g_hash_table_insert(partTable, GUINT_TO_POINTER(objMeta->part[i].number), GUINT_TO_POINTER(objMeta->part[i].size + size));
                    LogActivity(H3_DEBUG_MSG, "Updating %d from %zu to %zu\n", objMeta->part[i].number, size, objMeta->part[i].size + size);
                }
                else{
                    g_hash_table_insert(partTable, GUINT_TO_POINTER(objMeta->part[i].number), GUINT_TO_POINTER(objMeta->part[i].size));
                    LogActivity(H3_DEBUG_MSG, "Setting %d to %zu\n", objMeta->part[i].number, objMeta->part[i].size);
                }
            }

            // Create a buffer to store the parts
            *nParts = g_hash_table_size(partTable);
            *partInfoArray = malloc(*nParts * sizeof(H3_PartInfo));

            // Copy data from hash-table to buffer
            i=0;
            GHashTableIter iter;
            gpointer key, value;
            g_hash_table_iter_init (&iter, partTable);
            while (g_hash_table_iter_next (&iter, &key, &value)) {
                (*partInfoArray)[i].partNumber = GPOINTER_TO_UINT(key);
                (*partInfoArray)[i].size = GPOINTER_TO_UINT(value);
                i++;
            }

            status = H3_SUCCESS;

            g_hash_table_destroy(partTable);
            free(objMeta);
        }
    }
    free(multiMeta);
    return status;
}

KV_Status DeletePart(H3_Context* ctx, H3_ObjectMetadata* objMeta, uint32_t partNumber){
    uint i;
    KV_Status status = KV_SUCCESS;

    for(i=0; status == KV_SUCCESS && i<objMeta->nParts; ){
        if(objMeta->part[i].number == partNumber){
            H3_PartId partId;
            if((status = ctx->operation->delete(ctx->handle, PartToId(partId, objMeta->uuid, &objMeta->part[i]))) == KV_SUCCESS){
                if(objMeta->nParts > 1){
                    memmove(&objMeta->part[i], &objMeta->part[--objMeta->nParts], sizeof(H3_PartMetadata));
                }
                else
                    objMeta->nParts = 0;
            }
        }
        else
            i++;
    }

    return status;
}

// The offset is necessary in case we cannot allocate a large enough buffer for the whole part and we have to do it in segments.
// Note that in this case we do not overwrite, instead we simply append.
KV_Status CreatePart(H3_Context* ctx, H3_ObjectMetadata* objMeta, KV_Value value, size_t size, off_t offset, uint32_t partNumber){
    KV_Status status = KV_SUCCESS;
    uint32_t partSubNumber = offset/H3_PART_SIZE;
    off_t inPartOffset = offset%H3_PART_SIZE;

    while(size && status == KV_SUCCESS) {
    	H3_PartId partId;
    	size_t partSize = min((H3_PART_SIZE - inPartOffset), size);
    	int partIndex = objMeta->nParts;

    	CreatePartId(partId, objMeta->uuid, partNumber, partSubNumber);
        if( (status = ctx->operation->write(ctx->handle, partId, value, partSize)) == KV_SUCCESS){

            // Create/Update metadata entry
        	objMeta->part[partIndex].number = partNumber;
        	objMeta->part[partIndex].subNumber = partSubNumber++;
        	objMeta->part[partIndex].offset = 0;					// Will be adjusted when object is completed
        	objMeta->part[partIndex].size = inPartOffset + partSize;

            // Advance counters
        	size -= partSize;
            value += partSize;
            objMeta->nParts++;

            // Once we append to the last sub-part of the previous run
            // we create new sub-parts i.e. written from the beginning.
            inPartOffset = 0;
        }
        else
        	objMeta->isBad = 1;
    }

    return status;
}



/*! \brief  Create a single part of a multipart object from user data.
 *
 * Creates a part of a multipart object designated by a number. If a part with the same number exists it is replaced by the new one.
 *
 * @param[in]   handle             An h3lib handle
 * @param[in]   token              Authentication information
 * @param[in]   multipartId        The object id
 * @param[in]   partNumber         The part number
 * @param[in]   data               Pointer to user data
 * @param[in]   size               The size of the data buffer
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Multipart object does not exist
 * @result \b H3_FAILURE            User has no access or unable to access multipart object
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 *
 */
H3_Status H3_CreatePart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId, uint32_t partNumber, void* data, size_t size){

    // Argument check
    if(!handle || !token  || !multipartId || !data){
        return H3_INVALID_ARGS;
    }

    H3_Status status = H3_FAILURE;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    H3_UserId userId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    // Extract userId from token...
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    // ...and retrieve multipart metadata
    if((kvStatus = op->metadata_read(_handle, multipartId, 0, &value, &mSize)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_FAILURE )
        return H3_FAILURE;

    // Make sure user has access to the multipart-object
    H3_MultipartMetadata* multiMeta = (H3_MultipartMetadata*)value;
    if(GrantMultipartAccess(userId, multiMeta)){
        value = NULL; mSize = 0;
        if(op->metadata_read(_handle, multiMeta->objectId, 0, &value, &mSize) == KV_SUCCESS){

            // Delete previous version of said part if any
            H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
            if( DeletePart(ctx, objMeta, partNumber) == KV_SUCCESS) {

                // Expand object metadata if needed
                uint nParts = objMeta->nParts + (size + H3_PART_SIZE - 1)/H3_PART_SIZE;
                uint nBatch = (nParts + H3_PART_BATCH_SIZE - 1)/H3_PART_BATCH_SIZE;
                size_t objMetaSize = sizeof(H3_ObjectMetadata) + nBatch * H3_PART_BATCH_SIZE * sizeof(H3_PartMetadata);
                if(objMetaSize > mSize)
                    objMeta = ReAllocFreeOnFail(objMeta, objMetaSize);

                if(objMeta){
					// The object has already been modified thus we need to record its state
					kvStatus = CreatePart(ctx, objMeta, data, size, 0, partNumber);
					if(op->metadata_write(_handle, multiMeta->objectId, (KV_Value)objMeta, objMetaSize) == KV_SUCCESS && kvStatus == KV_SUCCESS){
						status = H3_SUCCESS;
					}
                }
            }

            // failed to delete all or some of the part's previous version so update the metadata
            else {
                op->metadata_write(_handle, multiMeta->objectId, (KV_Value)objMeta, mSize);
            }

            if(objMeta)
            	free(objMeta);
        }
    }

    free(multiMeta);
    return status;
}



/*! \brief  Create a single part of a multipart object from a pre-existing object.
 *
 * Creates a part of a multipart object designated by a number. If a part with the same number exists it is replaced by the new one.
 * The data are sourced from an ordinary object expected to be hosted in the same bucket as the multipart one.
 *
 * @param[in]   handle             An h3lib handle
 * @param[in]   token              Authentication information
 * @param[in]   objectName         The object name
 * @param[in]   offset             Offset with respect to the ordinary object's 0x00 byte
 * @param[in]   size               The size of the data to be copied
 * @param[in]   multipartId        The object id
 * @param[in]   partNumber         The part number
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Multipart or ordinary object does not exist
 * @result \b H3_FAILURE            User has no access or unable to access multipart object
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_NAME_TOO_LONG      Object name is longer than H3_OBJECT_NAME_SIZE
 *
 */
H3_Status H3_CreatePartCopy(H3_Handle handle, H3_Token token, H3_Name objectName, off_t offset, size_t size, H3_MultipartId multipartId, uint32_t partNumber){

    // Argument check.
    if(!handle || !token  || !multipartId || !objectName){
        return H3_INVALID_ARGS;
    }

    H3_Status status;
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    H3_UserId userId;
    KV_Status kvStatus;
    KV_Value value = NULL;
    size_t mSize = 0;

    if( (status = ValidObjectName(op, objectName)) != H3_SUCCESS){
        return status;
    }

    // Extract userId from token...
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    // ...and retrieve multipart metadata
    if((kvStatus = op->metadata_read(_handle, multipartId, 0, &value, &mSize)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_FAILURE )
        return H3_FAILURE;

    // Make sure user has access to the multipart-object
    status = H3_FAILURE;
    H3_MultipartMetadata* multiMeta = (H3_MultipartMetadata*)value;
    if( GrantMultipartAccess(userId, multiMeta)){
        H3_ObjectId srcObjId;
        H3_BucketId bucketName; // just the name not a proper ID


        GetBucketFromId(multiMeta->objectId, bucketName);
        GetObjectId(bucketName, objectName, srcObjId);
        value = NULL; mSize = 0;
        if((kvStatus = op->metadata_read(_handle, srcObjId, 0, &value, &mSize)) == KV_SUCCESS){
            H3_ObjectMetadata* srcObjMeta = (H3_ObjectMetadata*)value;

            value = NULL; mSize = 0;
            if(op->metadata_read(_handle, multiMeta->objectId, 0, &value, &mSize) == KV_SUCCESS){
                H3_ObjectMetadata* dstObjMeta = (H3_ObjectMetadata*)value;
                if( DeletePart(ctx, dstObjMeta, partNumber) == KV_SUCCESS) {

                    // Expand destination object metadata if needed
                    uint nParts = dstObjMeta->nParts + (size + H3_PART_SIZE - 1)/H3_PART_SIZE;
                    uint nBatch = (nParts + H3_PART_BATCH_SIZE - 1)/H3_PART_BATCH_SIZE;
                    size_t dstObjMetaSize = sizeof(H3_ObjectMetadata) + nBatch * H3_PART_BATCH_SIZE * sizeof(H3_PartMetadata);
                    if(dstObjMetaSize > mSize)
                        dstObjMeta = ReAllocFreeOnFail(dstObjMeta, dstObjMetaSize);

                    if(dstObjMeta){
						// Copy the data in parts
						KV_Value buffer = malloc(H3_PART_SIZE);
						size_t remaining = size;
						off_t srcOffset = offset;
						off_t dstOffset = 0;

						while(remaining && kvStatus == KV_SUCCESS){

							size_t buffSize = min(H3_PART_SIZE, remaining);
							if( (kvStatus = ReadData(ctx, srcObjMeta, buffer, &buffSize, srcOffset)) == KV_SUCCESS              &&
								(kvStatus = CreatePart(ctx, dstObjMeta, buffer, buffSize, dstOffset, partNumber)) == KV_SUCCESS     ){

								remaining -= buffSize;
								srcOffset += buffSize;
								dstOffset += buffSize;
							}
						}// while()


						// We have to update metadata even if writing failed because we might have already deleted the previous
						// version of the part.
						if(op->metadata_write(_handle, multiMeta->objectId, (KV_Value)dstObjMeta, dstObjMetaSize) == KV_SUCCESS){
							status = H3_SUCCESS;
						}
                    }
                }
                if(dstObjMeta)
                	free(dstObjMeta);
            }
            free(srcObjMeta);
        }
        else if(kvStatus == KV_KEY_NOT_EXIST)
            status = H3_NOT_EXISTS;

        else if(kvStatus == KV_KEY_TOO_LONG)
            status = H3_NAME_TOO_LONG;
    }

    free(multiMeta);
    return status;
}

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

int ValidBucketName(char* name){
    int status = TRUE;
    regex_t regex;

    // https://regex101.com/
    // Anything but / and # --> [/#]
    // Only from accepted characters --> [^_0-9a-zA-Z.-]

    // Too small/big
    size_t nameSize = strnlen(name, H3_BUCKET_NAME_SIZE+1);
    if( nameSize == 0 || nameSize > H3_BUCKET_NAME_SIZE     ||
        regcomp(&regex, "[^_0-9a-zA-Z.-]", 0) != REG_NOERROR    ){
        return FALSE;
    }

    // Contains invalid characters
    if(regexec(&regex, name, 0, NULL, 0) != REG_NOMATCH){
        status = FALSE;
    }

    regfree(&regex);
    return status;
}


/*! \brief Create a bucket
 *
 * Create a bucket associated with a specific user( derived from the token). The bucket name must not exceed a certain size
 * and may only consist of the following characters 0-9, a-z, A-Z, _ (underscore) , - (minus) and . (dot).
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to be created
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_EXISTS             Bucket already exists
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_STORE_ERROR        Storage provider error
 *
 */
H3_Status H3_CreateBucket(H3_Handle handle, H3_Token token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketId bucketId;
    H3_BucketMetadata bucketMetadata;
    H3_UserMetadata* userMetadata;
    KV_Value value = NULL;
    KV_Status status;

    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t metaSize = 0;


    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    // Populate bucket metadata
    memcpy(bucketMetadata.userId, userId, sizeof(H3_UserId));
    clock_gettime(CLOCK_REALTIME, &bucketMetadata.creation);
    bucketMetadata.mode = S_IFDIR | 0777;

    if( (status = op->metadata_create(_handle, bucketId, (KV_Value)&bucketMetadata, 0, sizeof(H3_BucketMetadata))) == KV_SUCCESS){

        if( (status = op->metadata_read(_handle, userId, 0, &value, &metaSize)) == KV_SUCCESS){
            // Extend existing user's metadata to fit new bucket-id if needed
            userMetadata = (H3_UserMetadata*)value;
            if(userMetadata->nBuckets == 0){
                metaSize = sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId);
                userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId));
            }
            else if(userMetadata->nBuckets % H3_BUCKET_BATCH_SIZE == 0){
                metaSize = sizeof(H3_UserMetadata) + (userMetadata->nBuckets + H3_BUCKET_BATCH_SIZE) * sizeof(H3_BucketId);
                userMetadata = realloc(userMetadata, metaSize);
            }
        }
        else if(status == KV_KEY_NOT_EXIST){
            // Create user's metadata i.e. create the user
            metaSize = sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId);
            userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId));
        }
        else {
            // Internal store error
            op->metadata_delete(_handle, bucketName);
            return H3_STORE_ERROR;
        }

        // Populate metadata and push them to the store
        strncpy(userMetadata->bucket[userMetadata->nBuckets++], bucketName, sizeof(H3_BucketId));
        op->metadata_write(_handle, userId, (KV_Value)userMetadata, 0, metaSize);
        free(userMetadata);

        return H3_SUCCESS;
    }
    else if(status == KV_KEY_EXIST)
        return H3_EXISTS;

    return H3_STORE_ERROR;
}




/*! \brief  Delete a bucket
 *
 * For the bucket to be deleted it must be empty and the token must grant access to it.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         The name of the bucket to be deleted
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         Bucket does not exist
 * @result \b H3_NOT_EMPTY         	Bucket is not empty
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error or the user has no access rights to this bucket
 *
 */
H3_Status H3_DeleteBucket(H3_Handle handle, H3_Token token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    uint32_t nKeys = 0;
    H3_Status status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    KV_Status kvStatus;
    size_t size = 0;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }


    if((kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }

    if(kvStatus == KV_SUCCESS){

        // Make sure the bucket is empty and the user has access to the bucket prior deletion
        H3_ObjectId prefix;
        GetObjectId(bucketName, NULL, prefix);
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        value = NULL; size = 0;
        if( GrantBucketAccess(userId, bucketMetadata)                              &&
            op->list(_handle, prefix, 0, NULL, 0, &nKeys) == KV_SUCCESS && !nKeys  &&
            op->metadata_read(_handle, userId, 0, &value, &size) == KV_SUCCESS     &&
            op->metadata_delete(_handle, bucketId) == KV_SUCCESS                     ){

            H3_UserMetadata* userMetadata = (H3_UserMetadata*)value;
            int index = GetBucketIndex(userMetadata, bucketName);
            if(index < userMetadata->nBuckets){
                // Remove bucket from metadata by copying the last entry into the slot occupied by the deleted one ...
                if(userMetadata->nBuckets > 1)
                    memmove(userMetadata->bucket[index], userMetadata->bucket[--userMetadata->nBuckets], sizeof(H3_BucketId));
                else
                    userMetadata->nBuckets = 0;

                // ... and shrink the array if needed
                if( userMetadata->nBuckets % H3_BUCKET_BATCH_SIZE == H3_BUCKET_BATCH_SIZE - 1){
                    // TODO - Implement shrinking
                }

                // Push the updated metadata to the store
                op->metadata_write(_handle, userId, (KV_Value)userMetadata, 0, size);
                status = H3_SUCCESS;
            }

            free(userMetadata);
        }
        else if(nKeys){
        	status = H3_NOT_EMPTY;
        }

        free(bucketMetadata);
    }

    return status;
}




/*! \brief List buckets associated with a user.
 *
 * The buffer containing the bucket names is allocated internally and should be freed by the user at the end.
 * In case of error, no buffer is generated. The names are null terminated strings of variable size, placed
 * back to back into the buffer.
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[out]   bucketNameArray    Pointer to a buffer that is to be filled with the buckets names expressed as null-terminated strings
 * @param[out]   nBuckets           Number of buckets contained in the buffer
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 *
 */
H3_Status H3_ListBuckets(H3_Handle handle, H3_Token token, H3_Name* bucketNameArray, uint32_t* nBuckets){

    // Argument check
    if(!handle || !token  || !bucketNameArray || !nBuckets){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    H3_UserId userId;
    KV_Status status;
    KV_Value value = NULL;
    size_t metaSize = 0;

    // Validate bucketName & extract userId from token
    if(!GetUserId(token, userId)){
        return H3_INVALID_ARGS;
    }

    if( (status = op->metadata_read(_handle, userId, 0, &value, &metaSize)) == KV_SUCCESS){
        H3_UserMetadata* userMetadata = (H3_UserMetadata*)value;
        size_t arraySize = metaSize - sizeof(H3_UserMetadata);

        *nBuckets = userMetadata->nBuckets;
        *bucketNameArray = malloc(arraySize);
        memcpy(*bucketNameArray, userMetadata->bucket, arraySize);
        free(userMetadata);

        return H3_SUCCESS;
    }
    else if(status == KV_KEY_NOT_EXIST){
        H3_UserMetadata userMetadata = {.nBuckets = 0};
        if(op->metadata_write(_handle, userId, (KV_Value)&userMetadata, 0, sizeof(H3_UserMetadata)) == KV_SUCCESS){
            *nBuckets = 0;
            return H3_SUCCESS;
        }
    }

    return H3_FAILURE;
}


/*! \brief Retrieve information about a bucket
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         Name of bucket
 * @param[inout] bucketInfo         User allocated structure to be filled with the info
 * @param[in]    getStats           If set, aggregate object information will also be produced at the cost of response time
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         The bucket doesn't exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 *
 */
H3_Status H3_InfoBucket(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_BucketInfo* bucketInfo, uint8_t getStats){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    size_t size = 0;
    H3_Status status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName || !bucketInfo){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId) ){
        return H3_INVALID_ARGS;
    }

    KV_Status kvStatus;
    if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;

        // Make sure the token grants access to the bucket
        if( GrantBucketAccess(userId, bucketMetadata) ){
            bucketInfo->creation = bucketMetadata->creation;
            bucketInfo->mode = bucketMetadata->mode;

            if(getStats){
                KV_Key keyBuffer = calloc(1, KV_LIST_BUFFER_SIZE);
                size_t bucketSize = 0;
                struct timespec lastAccess = {0,0};
                struct timespec lastModification = {0,0};
                H3_ObjectId prefix;
                uint32_t keyOffset = 0, nKeys = 0;

                // Apply no trim so we don't need to recreate the object-ID for the entries
                GetObjectId(bucketName, NULL, prefix);
                while((kvStatus = op->list(_handle, prefix, 0, keyBuffer, keyOffset, &nKeys)) == KV_CONTINUE || kvStatus == KV_SUCCESS){
                    uint32_t i = 0;
                    KV_Key objId = keyBuffer;

                    value = NULL; size = 0;
                    while(i < nKeys && (kvStatus = op->metadata_read(_handle, objId, 0, &value, &size)) == KV_SUCCESS){
                        H3_ObjectMetadata* objMeta = (H3_ObjectMetadata*)value;
                        if(objMeta->nParts){
                            bucketSize += objMeta->part[objMeta->nParts-1].offset + objMeta->part[objMeta->nParts-1].size;
                            lastAccess = Posterior(&lastAccess, &objMeta->lastAccess);
                            lastModification = Posterior(&lastModification, &objMeta->lastModification);
                        }

                        objId += strlen(objId)+1;
                        free(objMeta);
                        value = NULL; size = 0;
                        i++;
                    }

                    // It's not an error to get an empty list
                    if(!nKeys)
                        break;

                    keyOffset += nKeys;
                    nKeys = 0;
                }

                free(keyBuffer);
                if(kvStatus == KV_SUCCESS){
                    bucketInfo->stats.nObjects = keyOffset;
                    bucketInfo->stats.lastAccess = lastAccess;
                    bucketInfo->stats.lastModification = lastModification;
                    bucketInfo->stats.size = bucketSize;
                    status = H3_SUCCESS;
                }
            }
            else{
                status = H3_SUCCESS;
                memset(&bucketInfo->stats, 0, sizeof(H3_BucketStats));
            }
        }
        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST){
        status = H3_NOT_EXISTS;
    }

    return status;
}



/*! \brief Execute user function for each bucket
 *
 * Invoke the function for each bucket associated with the user, passing it the bucket name and user provided data.
 *
 * @param[in]   handle      An h3lib handle
 * @param[in]   token       Authentication information
 * @param[in]   function    User function to be invoked for each bucket
 * @param[in]   userData    User data to be passed to the function
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 *
 */
H3_Status H3_ForeachBucket(H3_Handle handle, H3_Token token, h3_name_iterator_cb function, void* userData){
    H3_UserId userId;
    KV_Value value = NULL;
    size_t size = 0;

    // Argument check
    if(!handle || !token  || !function){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if(!GetUserId(token, userId) ){
        return H3_INVALID_ARGS;
    }

    if( op->metadata_read(_handle, userId, 0, &value, &size) == KV_SUCCESS){
        H3_UserMetadata* userMetadata = (H3_UserMetadata*)value;

        // Call the user function
        int i;
        for(i=0; i<userMetadata->nBuckets; i++)
            function(userMetadata->bucket[i], userData);

        return H3_SUCCESS;
    }

    return H3_FAILURE;
}


/*! \brief Set a bucket's permission bits
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         Name of bucket
 * @param[in]    mode         		Permissions in octal mode similar to chmod()
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         The bucket doesn't exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 *
 */
H3_Status H3_SetBucketAttributes(H3_Handle handle, H3_Token token, H3_Name bucketName, mode_t mode){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    size_t size = 0;
    H3_Status status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if( !ValidBucketName(bucketName) || !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId) ){
        return H3_INVALID_ARGS;
    }

    KV_Status kvStatus;
    if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;

        // Make sure the token grants access to the bucket
        if( GrantBucketAccess(userId, bucketMetadata) ){
        	bucketMetadata->mode = mode & 0777;
        	if(op->metadata_write(_handle, bucketId, (KV_Value)bucketMetadata, 0, size) == KV_SUCCESS){
        		status = H3_SUCCESS;
        	}
        }
        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST){
        status = H3_NOT_EXISTS;
    }

    return status;
}

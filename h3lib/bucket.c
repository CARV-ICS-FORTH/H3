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

H3_Status ValidBucketName(KV_Operations* op, char* name){
    H3_Status status = H3_SUCCESS;

    // Too small/big
    size_t nameSize = strnlen(name, H3_BUCKET_NAME_SIZE+1);
    if(nameSize > H3_BUCKET_NAME_SIZE){
        status = H3_NAME_TOO_LONG;
    }

    // Check for invalid characters
    else if( nameSize == 0 || strpbrk(name, "/#") ||(op->validate_key && op->validate_key(name) != KV_SUCCESS)   ){
    	status = H3_INVALID_ARGS;
    }

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
 * @result \b H3_FAILURE        	Internal error
 * @result \b H3_EXISTS             Bucket already exists
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_STORE_ERROR        Storage provider error
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_CreateBucket(H3_Handle handle, H3_Token token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketId bucketId;
    H3_BucketMetadata bucketMetadata;
    H3_UserMetadata* userMetadata;
    KV_Value value = NULL;
    H3_Status status;


    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t metaSize = 0;
    KV_Status kvStatus;


    // Validate bucketName & extract userId from token
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    // Populate bucket metadata
    memcpy(bucketMetadata.userId, userId, sizeof(H3_UserId));
    clock_gettime(CLOCK_REALTIME, &bucketMetadata.creation);

    if( (kvStatus = op->metadata_create(_handle, bucketId, (KV_Value)&bucketMetadata, sizeof(H3_BucketMetadata))) == KV_SUCCESS){

        if( (kvStatus = op->metadata_read(_handle, userId, 0, &value, &metaSize)) == KV_SUCCESS){
            // Extend existing user's metadata to fit new bucket-id if needed
            userMetadata = (H3_UserMetadata*)value;
            if(userMetadata->nBuckets == 0){
                metaSize = sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId);
                userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId));
            }
            else if(userMetadata->nBuckets % H3_BUCKET_BATCH_SIZE == 0){
                metaSize = sizeof(H3_UserMetadata) + (userMetadata->nBuckets + H3_BUCKET_BATCH_SIZE) * sizeof(H3_BucketId);
                userMetadata = ReAllocFreeOnFail(userMetadata, metaSize);
            }
        }
        else if(kvStatus == KV_KEY_NOT_EXIST){
            // Create user's metadata i.e. create the user
            metaSize = sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId);
            userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId));
        }
        else {
            // Internal store error
            op->metadata_delete(_handle, bucketName);
            return H3_STORE_ERROR;
        }

        if(userMetadata){
			// Populate metadata and push them to the store
			strncpy(userMetadata->bucket[userMetadata->nBuckets++], bucketName, sizeof(H3_BucketId));
			op->metadata_write(_handle, userId, (KV_Value)userMetadata, metaSize);
			free(userMetadata);

			return H3_SUCCESS;
        }
        else
        	return H3_FAILURE;
    }
    else if(kvStatus == KV_KEY_EXIST)
        return H3_EXISTS;

    else if(kvStatus == KV_KEY_TOO_LONG)
        return H3_NAME_TOO_LONG;

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
 * @result \b H3_NOT_EMPTY          Bucket is not empty
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error or the user has no access rights to this bucket
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_DeleteBucket(H3_Handle handle, H3_Token token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    uint32_t nKeys = 0;
    H3_Status status;

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
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    status = H3_FAILURE;
    if((kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){

        // Make sure the bucket is empty and the user has access to the bucket prior deletion
        H3_ObjectId prefix;
        GetObjectId(bucketName, NULL, prefix);
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        value = NULL; size = 0;
        if( GrantBucketAccess(userId, bucketMetadata)                              &&

            (kvStatus = op->list(_handle, prefix, 0, NULL, 0, &nKeys)) == KV_SUCCESS && !nKeys  &&
            (kvStatus = op->metadata_read(_handle, userId, 0, &value, &size)) == KV_SUCCESS     &&
            (kvStatus = op->metadata_delete(_handle, bucketId)) == KV_SUCCESS                     ){

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
                op->metadata_write(_handle, userId, (KV_Value)userMetadata, size);
                status = H3_SUCCESS;
            }

            free(userMetadata);
        }
        else if(kvStatus == KV_KEY_TOO_LONG){
            return H3_NAME_TOO_LONG;
        }
        else if(nKeys){
            status = H3_NOT_EMPTY;
        }

        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_KEY_TOO_LONG)
        return H3_NAME_TOO_LONG;


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
        if(userMetadata->nBuckets){
            int i;
            char* entry;

            *nBuckets = userMetadata->nBuckets;
            *bucketNameArray = calloc(userMetadata->nBuckets, sizeof(H3_BucketId));
            entry = *bucketNameArray;

            for(i=0; i<userMetadata->nBuckets; i++){
                strcpy(entry, (const char*)userMetadata->bucket[i]);
                entry += strlen(entry) + 1;
            }
        }
        else {
            *nBuckets = 0;
            *bucketNameArray = NULL;
        }

        free(userMetadata);

        return H3_SUCCESS;
    }
    else if(status == KV_KEY_NOT_EXIST){
        H3_UserMetadata userMetadata = {.nBuckets = 0};
        if(op->metadata_write(_handle, userId, (KV_Value)&userMetadata, sizeof(H3_UserMetadata)) == KV_SUCCESS){
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
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_InfoBucket(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_BucketInfo* bucketInfo, uint8_t getStats){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    size_t size = 0;
    H3_Status status;
    KV_Status kvStatus;

    // Argument check
    if(!handle || !token  || !bucketName || !bucketInfo){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    status = H3_FAILURE;
    if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;

        // Make sure the token grants access to the bucket
        if( GrantBucketAccess(userId, bucketMetadata) ){
            bucketInfo->creation = bucketMetadata->creation;

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
                else if(kvStatus == KV_KEY_TOO_LONG)
                    status = H3_NAME_TOO_LONG;
            }
            else{
                status = H3_SUCCESS;
                memset(&bucketInfo->stats, 0, sizeof(H3_BucketStats));
            }
        }
        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_KEY_TOO_LONG)
        return H3_NAME_TOO_LONG;


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
 * @param[in]    attrib             Bucket attributes
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         The bucket doesn't exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_SetBucketAttributes(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Attribute attrib){
    H3_UserId userId;
    H3_BucketId bucketId;
    KV_Value value = NULL;
    size_t size = 0;
    H3_Status status;

    // Argument check
    if(!handle || !token  || !bucketName || attrib.type >= H3_NumOfAttributes){
        return H3_INVALID_ARGS;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    KV_Status kvStatus;

    // Validate bucketName & extract userId from token
    if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
        return status;
    }

    if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
        return H3_INVALID_ARGS;
    }

    if(attrib.type == H3_ATTRIBUTE_PERMISSIONS || attrib.type == H3_ATTRIBUTE_OWNER) {
        return H3_INVALID_ARGS;
    }

    status = H3_FAILURE;
    if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;

        // Make sure the token grants access to the bucket
        if( GrantBucketAccess(userId, bucketMetadata) ){
            // No attributes implemented yet

            if(op->metadata_write(_handle, bucketId, (KV_Value)bucketMetadata, size) == KV_SUCCESS){
                status = H3_SUCCESS;
            }
        }
        free(bucketMetadata);
    }
    else if(kvStatus == KV_KEY_NOT_EXIST){
        return H3_NOT_EXISTS;
    }
    else if(kvStatus == KV_KEY_TOO_LONG){
        return H3_NAME_TOO_LONG;
    }

    return status;
}


/*! \brief Delete all objects of a bucket
 *
 * @param[in]    handle             An h3lib handle
 * @param[in]    token              Authentication information
 * @param[in]    bucketName         Name of bucket
 *
 * @result \b H3_SUCCESS            Operation completed successfully
 * @result \b H3_NOT_EXISTS         The bucket doesn't exist
 * @result \b H3_INVALID_ARGS       Missing or malformed arguments
 * @result \b H3_FAILURE            Storage provider error
 * @result \b H3_NAME_TOO_LONG      Bucket name is longer than H3_BUCKET_NAME_SIZE
 *
 */
H3_Status H3_PurgeBucket(H3_Handle handle, H3_Token token, H3_Name bucketName){
	H3_UserId userId;
	H3_BucketId bucketId;
	KV_Value value = NULL;
	size_t size = 0;
	H3_Status status;
	KV_Status kvStatus;

	// Argument check
	if(!handle || !token  || !bucketName){
		return H3_INVALID_ARGS;
	}

	H3_Context* ctx = (H3_Context*)handle;
	KV_Handle _handle = ctx->handle;
	KV_Operations* op = ctx->operation;

	// Validate bucketName & extract userId from token
	if( (status = ValidBucketName(op, bucketName)) != H3_SUCCESS){
		return status;
	}

	if( !GetUserId(token, userId) || !GetBucketId(bucketName, bucketId)){
		return H3_INVALID_ARGS;
	}

	status = H3_FAILURE;
	if( (kvStatus = op->metadata_read(_handle, bucketId, 0, &value, &size)) == KV_SUCCESS){

		// Make sure the token grants access to the bucket
		H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
		if( GrantBucketAccess(userId, bucketMetadata) ){

			KV_Key keyBuffer = calloc(1, KV_LIST_BUFFER_SIZE);
			H3_ObjectId prefix;
			uint32_t nKeys = 0;

			// Apply no trim so we don't need to recreate the object-ID for the entries
			GetObjectId(bucketName, NULL, prefix);
			while((kvStatus = op->list(_handle, prefix, 0, keyBuffer, 0, &nKeys)) == KV_CONTINUE || kvStatus == KV_SUCCESS){
				uint32_t i = 0;
				KV_Key objId = keyBuffer;

				while(i < nKeys && DeleteObject(ctx, userId, objId, 0) == H3_SUCCESS){
//					LogActivity(H3_DEBUG_MSG, "Deleted %s\n", objId);
					objId += strlen(objId)+1;
					i++;
				}

				// Check for error in deletion
				if(i < nKeys){
					kvStatus = KV_FAILURE;
					break;
				}

				// It's not an error to get an empty list
				if(!nKeys)
					break;

				nKeys = 0;
			}

			free(keyBuffer);
			if(kvStatus == KV_SUCCESS){
				status = H3_SUCCESS;
			}
			else if(kvStatus == KV_KEY_TOO_LONG)
				status = H3_NAME_TOO_LONG;
		}
		free(bucketMetadata);
	}
	else if(kvStatus == KV_KEY_NOT_EXIST){
		return H3_NOT_EXISTS;
	}
	else if(kvStatus == KV_KEY_TOO_LONG)
		return H3_NAME_TOO_LONG;

	return status;
}

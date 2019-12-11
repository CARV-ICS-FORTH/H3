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

int ValidateBucketName(char* name){
    regex_t regex;

    // https://regex101.com/
    // Anything but / and # --> [/#]
    // Only from accepted characters --> [^_0-9a-zA-Z-]

    if( strnlen(name, H3_BUCKET_NAME_SIZE) == H3_BUCKET_NAME_SIZE   ||          // Too big
        regcomp(&regex, "[^_0-9a-zA-Z-]", 0)                        ||          // Contains invalid characters
        regexec(&regex, name, 0, NULL, 0) == REG_NOERROR                ){

        return H3_FAILURE;
    }

    return H3_SUCCESS;
}


int H3_CreateBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketMetadata bucketMetadata;
    H3_UserMetadata* userMetadata;
    KV_Value value = NULL;
    KV_Status status;

    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t size = 0;


    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    // Populate bucket metadata
    memcpy(bucketMetadata.userId, userId, sizeof(H3_UserId));
    bucketMetadata.creation = time(NULL);

    if( op->metadata_create(_handle, bucketName, (KV_Value)&bucketMetadata, 0, sizeof(H3_BucketMetadata)) == KV_SUCCESS){

        if( (status = op->metadata_read(_handle, userId, 0, &value, &size)) == KV_SUCCESS){
            // Extend existing user's metadata to fit new bucket-id if needed
            userMetadata = (H3_UserMetadata*)value;
            if(userMetadata->nBuckets % H3_BUCKET_BATCH_SIZE == H3_BUCKET_BATCH_SIZE - 1){
                size = sizeof(H3_UserMetadata) + ((userMetadata->nBuckets / H3_BUCKET_BATCH_SIZE) + 1) * sizeof(H3_BucketId);
                userMetadata = realloc(userMetadata, size);
            }
        }
        else if(status == KV_KEY_NOT_EXIST){
            // Create user's metadata i.e. create the user
            userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_BATCH_SIZE * sizeof(H3_BucketId));
        }
        else {
            // Internal store error
            op->metadata_delete(_handle, bucketName);
            return H3_FAILURE;
        }

        // Populate metadata and push them to the store
        strncpy(userMetadata->bucket[userMetadata->nBuckets++], bucketName, sizeof(H3_BucketId));
        op->metadata_write(_handle, userId, (KV_Value)userMetadata, 0, size);
        free(userMetadata);

        return H3_SUCCESS;
    }

    return H3_FAILURE;
}

int H3_DeleteBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){
    H3_UserId userId;
    KV_Value value = NULL;
    uint64_t nKeys;
    int status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t size = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    char prefix[H3_BUCKET_NAME_SIZE+1];
    sprintf(prefix, "%s/", bucketName);
    if( op->list(_handle, prefix, NULL, &nKeys) == KV_SUCCESS                   &&
        nKeys == 0                                                              &&
        op->metadata_read(_handle, bucketName, 0, &value, &size) == KV_SUCCESS      ){

        // Make sure user has access to the bucket prior deletion
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;
        if( GrantBucketAccess(userId, bucketMetadata)                           &&
            op->metadata_delete(_handle, bucketName) == KV_SUCCESS              &&
            op->metadata_read(_handle, userId, 0, &value, &size) == KV_SUCCESS      ){

            H3_UserMetadata* userMetadata = (H3_UserMetadata*)value;
            int index = GetBucketIndex(userMetadata, bucketName);
            if(index < userMetadata->nBuckets){
                // Remove bucket from metadata ...
                memcpy(userMetadata->bucket[index], userMetadata->bucket[--userMetadata->nBuckets], sizeof(H3_BucketId));

                // ... and shrink array if needed
                if( userMetadata->nBuckets % H3_BUCKET_BATCH_SIZE == H3_BUCKET_BATCH_SIZE - 1){
                    // TODO - Implement shrinking
                }

                // Push the updated metadata to the store
                op->metadata_write(_handle, userId, (KV_Value)userMetadata, 0, size);
                status = H3_SUCCESS;
            }
            free(userMetadata);
        }
        free(bucketMetadata);
    }
    return status;
}

int H3_ListBuckets(H3_Handle handle, H3_Token* token, H3_Name* bucketNames, size_t* size){
    H3_UserId userId;
    H3_UserMetadata* userMetadata;
    KV_Value value = NULL;
    size_t metaSize = 0;

    // Argument check
    if(!handle || !token  || !bucketNames || !size){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    if( op->metadata_read(_handle, userId, 0, &value, &metaSize) == KV_SUCCESS){
        userMetadata = (H3_UserMetadata*)value;
        *size = userMetadata->nBuckets;
        bucketNames = (H3_Name*)userMetadata->bucket;
        return H3_SUCCESS;
    }

    return H3_FAILURE;
}

int H3_InfoBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_BucketInfo* bucketInfo){
    H3_UserId userId;
    KV_Value value = NULL;
    size_t size = 0;
    int status = H3_FAILURE;

    // Argument check
    if(!handle || !token  || !bucketName || !bucketInfo){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    if( op->metadata_read(_handle, bucketName, 0, &value, &size) == KV_SUCCESS){
        H3_BucketMetadata* bucketMetadata = (H3_BucketMetadata*)value;

        // Make sure the token grants access to the bucket
        if( GrantBucketAccess(userId, bucketMetadata) ){
            bucketInfo->name = strdup(bucketName);
            bucketInfo->creation = bucketMetadata->creation;

            // TODO - Get the list of objects
            // TODO - Get info for each object in list
            bucketInfo->nObjects = 0;
            bucketInfo->size = 0;


            status = H3_SUCCESS;
        }
        free(bucketMetadata);
    }

    return status;
}


int H3_ForeachBucket(H3_Handle handle, H3_Token* token, h3_name_iterator_cb function, void* userData){
    H3_UserId userId;
    KV_Value value = NULL;
    size_t size = 0;

    // Argument check
    if(!handle || !token  || !function){
        return H3_FAILURE;
    }

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;

    // Validate bucketName & extract userId from token
    if(!GetUserId(token, userId) ){
        return H3_FAILURE;
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

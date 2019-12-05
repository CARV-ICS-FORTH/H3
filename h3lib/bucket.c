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
    KV_Value value;
    KV_Status status;

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
    bucketMetadata.lastAccess = bucketMetadata.creation;
    bucketMetadata.lastModification = bucketMetadata.creation;

    if( op->metadata_create(_handle, bucketName, (KV_Value)&bucketMetadata, 0, sizeof(H3_BucketMetadata)) == KV_SUCCESS){

        if( (status = op->metadata_read(_handle, userId, 0, &value, &size)) == KV_SUCCESS){
            // Extend existing user's metadata to fit new bucket-id if needed
            userMetadata = (H3_UserMetadata*)value;
            if(userMetadata->nBuckets % H3_BUCKET_NAME_BATCH_SIZE == H3_BUCKET_NAME_BATCH_SIZE - 1){
                size = sizeof(H3_UserMetadata) + ((userMetadata->nBuckets / H3_BUCKET_NAME_BATCH_SIZE) + 1) * sizeof(H3_BucketId);
                userMetadata = realloc(userMetadata, size);
            }
        }
        else if(status == KV_KEY_NOT_EXIST){
            // Create user's metadata i.e. create the user
            userMetadata = calloc(1, sizeof(H3_UserMetadata) + H3_BUCKET_NAME_BATCH_SIZE * sizeof(H3_BucketId));
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

int H3_InfoBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_BucketInfo* bucketInfo){return H3_FAILURE;}

int H3_DeleteBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){
    H3_UserId userId;
    H3_BucketMetadata bucketMetadata;
    H3_UserMetadata* userMetadata;
    KV_Value value;
    KV_Status status;

    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    KV_Operations* op = ctx->operation;
    size_t size = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) || !GetUserId(token, userId) ){
        return H3_FAILURE;
    }

    return H3_FAILURE;
}


int H3_ListBuckets(H3_Handle handle, H3_Token* token, size_t maxSize, uint64_t offset, H3_Name* bucketNames, size_t* size){return H3_FAILURE;}
int H3_ForeachBucket(H3_Handle handle, H3_Token* token, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}

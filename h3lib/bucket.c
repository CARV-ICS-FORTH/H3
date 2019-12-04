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
    H3_Context* ctx = (H3_Context*)handle;
    KV_Handle _handle = ctx->handle;
    H3_UserId userId;
    KV_Value value;
    int size = 0;

    // Validate bucketName & extract userId from token
    if( !ValidateBucketName(bucketName) ||
        !GetUserId(token, userId)           ){

        return H3_FAILURE;
    }

    ctx->operation->metadata_read(_handle, userId, 0, &value, &size);


//    user_metadata = get(key=user_id)
//    create(key=bucket_id, value=bucket_metadata)
//    user_metadata += bucket_id
//    put(key=user_id, value=user_metadata)
    return H3_SUCCESS;
}

int H3_InfoBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName, H3_BucketInfo* bucketInfo){return H3_FAILURE;}
int H3_DeleteBucket(H3_Handle handle, H3_Token* token, H3_Name bucketName){return H3_FAILURE;}
int H3_ListBuckets(H3_Handle handle, H3_Token* token, int maxSize, int offset, H3_Name* bucketNames, int* size){return H3_FAILURE;}
int H3_ForeachBucket(H3_Handle handle, H3_Token* token, h3_name_iterator_cb function, void* userData){return H3_FAILURE;}

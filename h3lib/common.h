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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>

#include <glib.h>

#include "h3lib.h"
#include "h3lib_config.h"
#include "kv_interface.h"

#define BUFF_SIZE 24
#define H3_SYSTEM_ID    0x00

#define H3_BUCKET_NAME_SIZE 64
#define H3_OBJECT_NAME_SIZE 512

#define H3_BUCKET_NAME_BATCH_SIZE   10

#define H3_USERID_SIZE      (H3_OBJECT_NAME_SIZE - 1)


typedef char H3_UserId[H3_USERID_SIZE];
typedef char H3_BucketId[H3_BUCKET_NAME_SIZE];
typedef char H3_ObjectId[H3_BUCKET_NAME_SIZE + H3_BUCKET_NAME_SIZE + 1];

typedef struct {
    H3_StoreType type;

    // Store specific
    KV_Handle handle;
    KV_Operations* operation;
}H3_Context;

typedef struct{
    uint nBuckets;
    H3_BucketId bucket[];
}H3_UserMetadata;

typedef struct{
    H3_UserId userId;
    time_t creation;
    time_t lastAccess;
    time_t lastModification;
}H3_BucketMetadata;



//#define mSizeofUserMetadata(a) (sizeof(H3_UserMetadata) + (a)->nBuckets * sizeof(H3_BucketId))




int ValidateBucketName(char* name);
int GetUserId(H3_Token* token, H3_UserId id);
int GetBucketIndex(H3_UserMetadata* userMetadata, H3_Name bucketName);

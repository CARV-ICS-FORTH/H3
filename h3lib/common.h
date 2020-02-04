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

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>

#include <uuid/uuid.h>

#include <glib.h>
#include <glib/gi18n.h>

#include "h3lib.h"
#include "h3lib_config.h"
#include "kv_interface.h"


#ifndef TRUE
#define TRUE    1
#endif

#ifndef FALSE
#define FALSE !TRUE
#endif

#ifndef UUID_STR_LEN
#define UUID_STR_LEN 36
#endif

#ifndef REG_NOERROR
#define REG_NOERROR 0
#endif

#define H3_PART_SIZE (1048576 * 2) // = 2Mb - Key - 4Kb kreon metadata
#define H3_SYSTEM_ID    0x00

#define H3_BUCKET_BATCH_SIZE   10
#define H3_PART_BATCH_SIZE   10

#define H3_USERID_SIZE      128
#define H3_MULIPARTID_SIZE  (UUID_STR_LEN + 1)


// Use typeof to make sure each argument is evaluated only once
// https://gcc.gnu.org/onlinedocs/gcc-4.9.2/gcc/Typeof.html#Typeof
#define max(a,b) ({ __typeof__ (a) _a = (a); __typeof__ (b) _b = (b); _a > _b ? _a : _b; })
#define min(a,b) ({ __typeof__ (a) _a = (a); __typeof__ (b) _b = (b); _a < _b ? _a : _b; })

typedef char H3_UserId[H3_USERID_SIZE+1];
typedef char H3_BucketId[H3_BUCKET_NAME_SIZE+2];
typedef char H3_ObjectId[H3_BUCKET_NAME_SIZE + H3_OBJECT_NAME_SIZE + 1];
typedef char H3_UUID[UUID_STR_LEN];
typedef char H3_PartId[50];                                                 // '_' + UUID[36+1byte] + '#' + <part_number> + ['.' + <subpart_number>]


typedef enum {
    DivideInParts, DivideInSubParts
}H3_PartitionPolicy;

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
}H3_BucketMetadata;

typedef struct{
    uint number;
    int subNumber;
    size_t size;
    off_t offset;  // For multipart uploads, the offset is set when the upload completes
}H3_PartMetadata;

typedef struct{
    char isBad;
    H3_UserId userId;
    uuid_t uuid;
    time_t creation;
    time_t lastAccess;
    time_t lastModification;
    uint nParts;
    H3_PartMetadata part[];
}H3_ObjectMetadata;

typedef struct{
    H3_UserId userId;
    H3_ObjectId objectId;
}H3_MultipartMetadata;


int ValidBucketName(char* name);
int ValidObjectName(char* name);
int GetUserId(H3_Token token, H3_UserId id);
int GetBucketId(H3_Name bucketName, H3_BucketId id);
int GetBucketIndex(H3_UserMetadata* userMetadata, H3_Name bucketName);
void GetObjectId(H3_Name bucketName, H3_Name objectName, H3_ObjectId id);
void GetMultipartObjectId(H3_Name bucketName, H3_Name objectName, H3_ObjectId id);
char* GetBucketFromId(H3_ObjectId objId, H3_BucketId bucketId);
H3_MultipartId GeneratetMultipartId(uuid_t uuid);
void CreatePartId(H3_PartId partId, uuid_t uuid, int partNumber, int subPartNumber);
char* PartToId(H3_PartId partId, uuid_t uuid, H3_PartMetadata* part);
int GrantBucketAccess(H3_UserId id, H3_BucketMetadata* meta);
int GrantObjectAccess(H3_UserId id, H3_ObjectMetadata* meta);
int GrantMultipartAccess(H3_UserId id, H3_MultipartMetadata* meta);
char* ConvertToOdrinary(H3_ObjectId id);
H3_Status DeleteObject(H3_Context* ctx, H3_UserId userId, H3_ObjectId objId);
KV_Status WriteData(H3_Context* ctx, H3_ObjectMetadata* meta, KV_Value value, size_t size, off_t offset, uint initialPartNumber, H3_PartitionPolicy policy);
KV_Status ReadData(H3_Context* ctx, H3_ObjectMetadata* meta, KV_Value value, size_t* size, off_t offset);
KV_Status CopyData(H3_Context* ctx, H3_UserId userId, H3_ObjectId srcObjId, H3_ObjectId dstObjId, off_t srcOffset, size_t size, uint8_t noOverwrite, off_t dstOffset);

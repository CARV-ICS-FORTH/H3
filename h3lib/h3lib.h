/*!
 * @file h3lib.h
 */

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

#ifndef H3LIB_H_
#define H3LIB_H_

#define _GNU_SOURCE
#include <stdint.h>
#include <time.h>
#include <unistd.h>

/** \defgroup Macros
 *  @{
 */
#define H3_BUCKET_NAME_SIZE    64   //!< Maximum number of characters allowed for a bucket
#define H3_OBJECT_NAME_SIZE    512  //!< Maximum number of characters allowed for an object
#define H3_METADATA_NAME_SIZE  64   //!< Maximum number of characters allowed for an object's metadata name
/** @}*/


/** \defgroup Typedefs
 *  @{
 */
typedef void* H3_Handle;                                            //!< Opaque pointer to an h3lib handle
typedef char* H3_Name;                                              //!< Alias to null terminated string
typedef char* H3_MultipartId;                                       //!< Alias to null terminated string
typedef void (*h3_name_iterator_cb)(H3_Name name, void* userData);  //!< User function to be invoked for each bucket
/** @}*/


/** \defgroup Enumerations
 *  @{
 */

/*! \brief H3 status/error codes */
typedef enum {
    H3_FAILURE = 0,     //!< Operation failed
    H3_INVALID_ARGS,    //!< Arguments are missing or malformed
    H3_STORE_ERROR,     //!< External (store provider) error
    H3_EXISTS,          //!< Bucket or object already exists
    H3_NOT_EXISTS,      //!< Bucket or object does not exist
    H3_NAME_TOO_LONG,   //!< Bucket or object name is too long
    H3_NOT_EMPTY,       //!< Bucket is not empty
    H3_SUCCESS,         //!< Operation succeeded
    H3_CONTINUE         //!< Operation succeeded though there are more data to retrieve
} H3_Status;

/*! \brief Object/Bucket attributes supported by H3 */
typedef enum {
    H3_ATTRIBUTE_PERMISSIONS = 0,   //!< Permissions attribute
    H3_ATTRIBUTE_OWNER,             //!< Owner attributes
    H3_ATTRIBUTE_READ_ONLY,         //!< Read only attribute
    H3_NumOfAttributes              //!< Not an option, used for iteration purposes
}H3_AttributeType;

/** @}*/

/*! \brief User authentication info */
typedef struct{
    uint32_t userId;
} H3_Auth;

/*! \brief Pointer to user authentication data */
typedef const H3_Auth* H3_Token;

/*! \brief H3 Storage information */
typedef struct {
    unsigned long totalSpace;
	unsigned long freeSpace;
	unsigned long usedSpace;
} H3_StorageInfo;

/*! \brief Bucket statistics */
typedef struct {
    size_t size;                         //!< The size of all objects contained in the bucket
    uint64_t nObjects;                   //!< Number of objects contained in the bucket
    struct timespec lastAccess;          //!< Last time an object was accessed
    struct timespec lastModification;    //!< Last time an object was modified
} H3_BucketStats;


/*! \brief Bucket information */
typedef struct {
    struct timespec creation;       //!< Creation timestamp
    H3_BucketStats  stats;          //!< Aggregate object statistics
} H3_BucketInfo;


/*! \brief H3 object information */
typedef struct {
    char isBad;                             //!< Data are corrupt
    char readOnly;                          //!< The object is read only (used by the h3controllers)
    size_t size;                            //!< Object size
    struct timespec creation;               //!< Creation timestamp
    struct timespec lastAccess;             //!< The last time the object was read
    struct timespec lastModification;       //!< The last time the object was modified (content has been modified)
    struct timespec lastChange;             //!< The last time the object's attributes were changed (e.g. permissions)
    mode_t mode;                            //!< File type and mode (used by h3fuse)
    uid_t uid;                              //!< User ID (used by h3fuse)
    gid_t gid;                              //!< Group ID (used by h3fuse)
} H3_ObjectInfo;


/*! \brief Information on individual parts of a multi-part object */
typedef struct {
    uint32_t partNumber;    //!< Part number
    size_t size;            //!< Part size
} H3_PartInfo;


/*! \brief Object & Bucket attributes */
typedef struct {
    H3_AttributeType type;
    union{
        mode_t mode;        //!< Permissions in octal mode similar to chmod()
        struct {
            uid_t uid;      //!< User ID, adhering to chown() semantics
            gid_t gid;      //!< Group ID, adhering to chown() semantics
        };
        char readOnly;      //!< This is used from the h3controllers, it is different from the mode  
    };
}H3_Attribute;


/** \defgroup Functions
 *  @{
 */

/*! Return the version string
 * \return Null terminated string
 */
char* H3_Version();

/** \defgroup handle Handle management
 *  @{
 */
H3_Handle H3_Init(const char* storageUri);
void H3_Free(H3_Handle handle);
/** @}*/

/** \defgroup storage Storage Info
 *  @{
 */
 H3_Status H3_InfoStorage(H3_Handle handle, H3_StorageInfo* storageInfo);
/** @}*/

/** \defgroup bucket Bucket management
 *  @{
 */
H3_Status H3_ListBuckets(H3_Handle handle, H3_Token token, H3_Name* bucketNameArray, uint32_t* nBuckets);
H3_Status H3_ForeachBucket(H3_Handle handle, H3_Token token, h3_name_iterator_cb function, void* userData);
H3_Status H3_InfoBucket(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_BucketInfo* bucketInfo, uint8_t getStats);
H3_Status H3_SetBucketAttributes(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Attribute attrib);
H3_Status H3_CreateBucket(H3_Handle handle, H3_Token token, H3_Name bucketName);
H3_Status H3_DeleteBucket(H3_Handle handle, H3_Token token, H3_Name bucketName);
H3_Status H3_PurgeBucket(H3_Handle handle, H3_Token token, H3_Name bucketName);
/** @}*/

/** \defgroup object Object management
 *  @{
 */
H3_Status H3_ListObjects(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t offset, H3_Name* objectNameArray, uint32_t* nObjects);
H3_Status H3_ForeachObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t nObjects, uint32_t offset, h3_name_iterator_cb function, void* userData);
H3_Status H3_InfoObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo);
H3_Status H3_ObjectExists(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName);
H3_Status H3_TouchObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, struct timespec *lastAccess, struct timespec *lastModification);
H3_Status H3_SetObjectAttributes(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_Attribute attrib);
H3_Status H3_CreateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size);
H3_Status H3_CreatePseudoObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* info);
H3_Status H3_CreateObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t offset, size_t* size, H3_Name dstObjectName);
H3_Status H3_CreateObjectFromFile(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, int fd, size_t size);
H3_Status H3_CreateDummyObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, const void* buffer, size_t bufferSize, size_t objectSize);
H3_Status H3_WriteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, off_t offset);
H3_Status H3_WriteObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t srcOffset, size_t* size, H3_Name dstObjectName, off_t dstOffset);
H3_Status H3_WriteObjectFromFile(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, int fd, size_t size, off_t offset);
H3_Status H3_ReadObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, off_t offset, void** data, size_t* size);
H3_Status H3_ReadDummyObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, size_t* size);
H3_Status H3_ReadObjectToFile(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, off_t offset, int fd, size_t* size);
H3_Status H3_CopyObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, uint8_t noOverwrite);
H3_Status H3_MoveObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, uint8_t noOverwrite);
H3_Status H3_ExchangeObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
H3_Status H3_TruncateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, size_t size);
H3_Status H3_DeleteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName);
H3_Status H3_CreateObjectMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_Name metadataName, void* data, size_t size);
H3_Status H3_ReadObjectMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_Name metadataName, void** data, size_t* size);
H3_Status H3_DeleteObjectMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_Name metadataName);
H3_Status H3_CopyObjectMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
H3_Status H3_MoveObjectMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName);
H3_Status H3_ListObjectsWithMetadata(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name metadataName, uint32_t offset, H3_Name* objectNameArray, uint32_t* nObjects, uint32_t* next0ffset);
/** @}*/


/** \defgroup multipart Multipart management
 *  @{
 */
H3_Status H3_ListMultiparts(H3_Handle handle, H3_Token token, H3_Name bucketName, uint32_t offset, H3_MultipartId* multipartIdArray, uint32_t* nIds);
H3_Status H3_CreateMultipart(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_MultipartId* multipartId);
H3_Status H3_CompleteMultipart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId);
H3_Status H3_AbortMultipart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId);
H3_Status H3_ListParts(H3_Handle handle, H3_Token token, H3_MultipartId multipartId, H3_PartInfo** partInfoArray, uint32_t* nParts);
H3_Status H3_CreatePart(H3_Handle handle, H3_Token token, H3_MultipartId multipartId, uint32_t partNumber, void* data, size_t size);
H3_Status H3_CreatePartCopy(H3_Handle handle, H3_Token token, H3_Name objectName, off_t offset, size_t size, H3_MultipartId multipartId, uint32_t partNumber);
/** @}*/
/** @}*/




#endif /* H3LIB_H_ */

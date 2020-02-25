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

#include <stdint.h>
#include <time.h>

/** \defgroup Macros
 *  @{
 */
#define H3_BUCKET_NAME_SIZE 64      //!< Maximum number of characters allowed for a bucket
#define H3_OBJECT_NAME_SIZE 512     //!< Maximum number of characters allowed for an object
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
	H3_NOT_EMPTY,       //!< Bucket is not empty
    H3_SUCCESS,         //!< Operation succeeded
    H3_CONTINUE         //!< Operation succeeded though there are more data to retrieve
} H3_Status;

/*! \brief Storage providers supported by H3 */
typedef enum {
    H3_STORE_CONFIG = 0,    //!< Provider is set in the configuration file
    H3_STORE_FILESYSTEM,    //!< Mounted filesystem
    H3_STORE_KREON,         //!< Kreon cluster  (not available)
    H3_STORE_ROCKSDB,       //!< RocksDB server (not available)
    H3_STORE_REDIS,         //!< Redis cluster  (not available)
    H3_STORE_IME,           //!< IME cluster    (not available)
    H3_STORE_NumOfStores    //!< Not an option, used for iteration purposes.
} H3_StoreType;


/*! \brief object rename policies supported by H3 */
typedef enum {
	H3_MOVE_REPLACE,	//!< Overwrite destination if exists
	H3_MOVE_NOREPLACE,	//!< Do not overwrite destination if exists
	H3_MOVE_EXCHANGE	//!< Swap data with destination (must exist)
}H3_MovePolicy;


/** @}*/

/*! \brief User authentication info */
typedef struct{
    uint32_t userId;
} H3_Auth;

/*! \brief Pointer to user authentication data */
typedef H3_Auth* H3_Token;


/*! \brief Bucket statistics */
typedef struct {
    size_t size;                //!< The size of all objects contained in the bucket
    uint64_t nObjects;          //!< Number of objects contained in the bucket
    time_t lastAccess;          //!< Last time an object was accessed
    time_t lastModification;    //!< Last time an object was modified
} H3_BucketStats;


/*! \brief Bucket information */
typedef struct {
    time_t creation;        //!< Creation timestamp
    H3_BucketStats stats;   //!< Aggregate object statistics
} H3_BucketInfo;


/*! \brief H3 object information */
typedef struct {
    char isBad;                 //!< Data are corrupt
    size_t size;                //!< Object size
    time_t creation;            //!< Creation timestamp
    time_t lastAccess;          //!< Last access timestamp
    time_t lastModification;    //!< Last modification timestamp
} H3_ObjectInfo;


/*! \brief Information on individual parts of a multi-part object */
typedef struct {
    uint32_t partNumber;    //!< Part number
    size_t size;            //!< Part size
} H3_PartInfo;



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
H3_Handle H3_Init(H3_StoreType storageType, char* cfgFileName);
void H3_Free(H3_Handle handle);
/** @}*/


/** \defgroup bucket Bucket management
 *  @{
 */
H3_Status H3_ListBuckets(H3_Handle handle, H3_Token token, H3_Name* bucketNameArray, uint32_t* nBuckets);
H3_Status H3_ForeachBucket(H3_Handle handle, H3_Token token, h3_name_iterator_cb function, void* userData);
H3_Status H3_InfoBucket(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_BucketInfo* bucketInfo, uint8_t getStats);
H3_Status H3_CreateBucket(H3_Handle handle, H3_Token token, H3_Name bucketName);
H3_Status H3_DeleteBucket(H3_Handle handle, H3_Token token, H3_Name bucketName);
/** @}*/

/** \defgroup object Object management
 *  @{
 */
H3_Status H3_ListObjects(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t offset, H3_Name* objectNameArray, uint32_t* nObjects);
H3_Status H3_ForeachObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name prefix, uint32_t nObjects, uint32_t offset, h3_name_iterator_cb function, void* userData);
H3_Status H3_InfoObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, H3_ObjectInfo* objectInfo);
H3_Status H3_CreateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size);
H3_Status H3_TruncateObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, size_t size);
H3_Status H3_CreateObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t offset, size_t* size, H3_Name dstObjectName);
H3_Status H3_WriteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, void* data, size_t size, off_t offset);
H3_Status H3_WriteObjectCopy(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, off_t srcOffset, size_t* size, H3_Name dstObjectName, off_t dstOffset);
H3_Status H3_ReadObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName, off_t offset, void** data, size_t* size);
H3_Status H3_CopyObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, uint8_t noOverwrite);
H3_Status H3_MoveObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name srcObjectName, H3_Name dstObjectName, H3_MovePolicy policy);
H3_Status H3_DeleteObject(H3_Handle handle, H3_Token token, H3_Name bucketName, H3_Name objectName);
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

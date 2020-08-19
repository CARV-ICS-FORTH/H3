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
#include <hiredis/hiredis.h>

#include "kv_interface.h"

#include "util.h"
#include "url_parser.h"



typedef struct {
	redisContext* ctx;
}KV_Redis_Handle;


KV_Handle KV_Redis_Init(const char* storageUri) {
    struct parsed_url *url = parse_url(storageUri);
    if (url == NULL) {
        LogActivity(H3_ERROR_MSG, "ERROR: Unrecognized storage URI\n");
        return NULL;
    }

    char *host;
    if (url->host != NULL) {
        host = strdup(url->host);
        LogActivity(H3_INFO_MSG, "INFO: Host in URI: %s\n", host);
    } else {
        host = strdup("127.0.0.1");
        LogActivity(H3_INFO_MSG, "WARNING: No host in URI. Using default: 127.0.0.1\n");
    }
    int port;
    if (url->port != NULL) {
        port = atoi(url->port);
        if (port == 0) {
            port = 6379;
            LogActivity(H3_INFO_MSG, "WARNING: Unrecognized port in URI. Using default: 6379\n");
        }
    } else {
        port = 6379;
        LogActivity(H3_INFO_MSG, "WARNING: No port in URI. Using default: 6379\n");
    }
    parsed_url_free(url);

    KV_Redis_Handle* handle = malloc(sizeof(KV_Redis_Handle));
    if (!handle)
        return NULL;

    if (!(handle->ctx = redisConnect(host, port))) {
        LogActivity(H3_ERROR_MSG, "HIREDIS - %s\n", handle->ctx->errstr);
        free(host);
        free(handle);
        return NULL;
    }

    free(host);
    return (KV_Handle)handle;
}

void KV_Redis_Free(KV_Handle handle) {
	KV_Redis_Handle* _handle = (KV_Redis_Handle*) handle;
	redisFree(_handle->ctx);
    free(_handle);
    return;
}

KV_Status KV_Redis_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
	KV_Status status = KV_SUCCESS;
   uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
   uint32_t nMatchingKeys = 0;
   size_t remaining = KV_LIST_BUFFER_SIZE;

   redisReply* reply = NULL;
   char* cursor = strdup("0");

   if(buffer)
   	memset(buffer, 0, KV_LIST_BUFFER_SIZE);

   do{
   	freeReplyObject(reply);
   	if((reply = redisCommand(storeHandle->ctx, "SCAN %s MATCH %s*", cursor, prefix))){
   		int i;

   		for(i=0; i<reply->element[1]->elements && status != KV_CONTINUE; i++){

   			if(offset)
   				offset--;

   			else if( nMatchingKeys < nRequiredKeys ){

   				// Copy the keys if a buffer is provided...
   				if(buffer){
   					size_t entrySize = reply->element[1]->element[i]->len - nTrim;
   					if(remaining >= (entrySize + 1) ){
   						memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &reply->element[1]->element[i]->str[nTrim], entrySize);
   						remaining -= (entrySize+1);
   						nMatchingKeys++;
   					}
   					else
   						status = KV_CONTINUE;
   				}

   				// ... otherwise just count them.
   				else
   					nMatchingKeys++;
   			}
   			else
   				status = KV_CONTINUE;
   		}

   		cursor = strdup(reply->element[0]->str);
   	}

   }while(reply && strcmp(cursor, "0") != 0 && status != KV_CONTINUE);

   if(reply){
   	freeReplyObject(reply);
   	*nKeys = nMatchingKeys;
   }
   else
   	status = KV_FAILURE;

   return status;
}

KV_Status KV_Redis_Exists(KV_Handle handle, KV_Key key) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
    KV_Status status = KV_FAILURE;
    redisReply* reply;

    if((reply = redisCommand(storeHandle->ctx, "EXISTS %s", key))){

    	if(reply->integer == 0)
    		status = KV_KEY_NOT_EXIST;
    	else
    		status = KV_KEY_EXIST;

    	freeReplyObject(reply);
    }

    return status;
}

KV_Status KV_Redis_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
	KV_Status status = KV_FAILURE;
	redisReply* reply;

	if(offset)
		reply = redisCommand(storeHandle->ctx, "GETRANGE %s %d %d", key, offset, size);
	else
		reply = redisCommand(storeHandle->ctx, "GET %s", key);

	if(reply){
		switch(reply->type){
			case REDIS_REPLY_NIL:
				status = KV_KEY_NOT_EXIST;
				break;

			case REDIS_REPLY_STRING:
				if(*value == NULL){
					*value = malloc(reply->len);
					*size = reply->len;
				}

				if(*value){
					*size = min(reply->len, *size);
					memcpy(*value, reply->str, *size);
					status = KV_SUCCESS;
				}
				break;
		}
		freeReplyObject(reply);
	}

	return status;
}

KV_Status KV_Redis_Create(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size){
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
	KV_Status status = KV_FAILURE;
	redisReply* reply;

	if(offset){

		// Attempt to reserve the key...
		if((reply = redisCommand(storeHandle->ctx, "SET %s %d NX", key, 0))){

			freeReplyObject(reply);
			if(reply->type == REDIS_REPLY_STRING ){

				//... and then populate it
				reply = redisCommand(storeHandle->ctx, "SETRANGE %s %d %b", key, offset, value, size);
			}
			else{
				freeReplyObject(reply);
				return KV_KEY_EXIST;
			}
		}
	}
	else
		reply = redisCommand(storeHandle->ctx, "SET %s %b NX", key, value, size);

	if(reply){
		switch(reply->type){
			case REDIS_REPLY_NIL: 			// SET
				status = KV_KEY_EXIST;
				break;

			case REDIS_REPLY_STATUS:		// SET
			case REDIS_REPLY_INTEGER: 		// SETRANGE
				status = KV_SUCCESS;
				break;
		}
		freeReplyObject(reply);
	}

	return status;
}

KV_Status KV_Redis_Write(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
	KV_Status status = KV_FAILURE;
	redisReply* reply;

	if(offset)
		reply = redisCommand(storeHandle->ctx, "SETRANGE %s %d %b", key, offset, value, size);
	else
		reply = redisCommand(storeHandle->ctx, "SET %s %b", key, value, size);

	if(reply){
		switch(reply->type){
			case REDIS_REPLY_STATUS:		// SET
			case REDIS_REPLY_INTEGER: 		// SETRANGE
				status = KV_SUCCESS;
				break;
		}
		freeReplyObject(reply);
	}

	return status;
}

KV_Status KV_Redis_Copy(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
    KV_Status status = KV_FAILURE;
    redisReply *setReply, *getReply;

    // NOTE: Command RESTORE does not work
    if((getReply = redisCommand(storeHandle->ctx, "GET %s", src_key))){
    	if(getReply->type == REDIS_REPLY_STRING){
    		if((setReply = redisCommand(storeHandle->ctx, "SET %s %b", dest_key, getReply->str, getReply->len))){
    			if(setReply->type == REDIS_REPLY_STATUS)
    				status = KV_SUCCESS;

    			freeReplyObject(setReply);
    		}
    	}
    	else
    		status = KV_KEY_NOT_EXIST;

    	freeReplyObject(getReply);
    }

    return status;
}

KV_Status KV_Redis_Delete(KV_Handle handle, KV_Key key) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
    KV_Status status = KV_FAILURE;
    redisReply* reply;

    if((reply = redisCommand(storeHandle->ctx, "DEL %s", key))){

    	if(reply->integer == 0)
    		status = KV_KEY_NOT_EXIST;
    	else
    		status = KV_SUCCESS;

    	freeReplyObject(reply);
    }

    return status;
}


KV_Status KV_Redis_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
//  KV_Redis_Cluster_Handle* storeHandle = (KV_Redis_Cluster_Handle*) handle;
//  KV_Status status = KV_FAILURE;
//  redisReply* reply;
//
//  if((reply = redisClusterCommand(storeHandle->ctx, "RENAME %s %s", src_key, dest_key))){
//      switch(reply->type){
//          case REDIS_REPLY_STRING:
//              status = KV_SUCCESS;
//              break;
//
//          case REDIS_REPLY_ERROR:
//              status = KV_KEY_NOT_EXIST;
//              break;
//      }
//
//      freeReplyObject(reply);
//  }
//
//  return status;

    // XXX Quick implementation
    if(KV_Redis_Copy(handle, src_key, dest_key) == KV_SUCCESS)
        return KV_Redis_Delete(handle, src_key);

    return KV_FAILURE;
}


KV_Status KV_Redis_Sync(KV_Handle handle) {
    return KV_FAILURE;
}


KV_Operations operationsRedis = {
    .init = KV_Redis_Init,
    .free = KV_Redis_Free,
	.validate_key = NULL,

    .metadata_read = KV_Redis_Read,
    .metadata_write = KV_Redis_Write,
    .metadata_create = KV_Redis_Create,
    .metadata_delete = KV_Redis_Delete,
    .metadata_move = KV_Redis_Move,
    .metadata_exists = KV_Redis_Exists,

    .list = KV_Redis_List,
    .exists = KV_Redis_Exists,
    .read = KV_Redis_Read,
    .create = KV_Redis_Create,
    .write = KV_Redis_Write,
    .copy = KV_Redis_Copy,
    .move = KV_Redis_Move,
    .delete = KV_Redis_Delete,
    .sync = KV_Redis_Sync
};

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
#include<hiredis-vip/hircluster.h>
#include<hiredis-vip/dict.h>

#include "kv_interface.h"

#include "util.h"



typedef struct {
	redisClusterContext* ctx;
}KV_Redis_Handle;


KV_Handle KV_Redis_Init(GKeyFile* cfgFile) {

    g_autoptr(GError) error = NULL;

	char* cluster = g_key_file_get_string (cfgFile, "REDIS", "nodes", &error);
    if (cluster == NULL) {
        if (g_error_matches(error, G_KEY_FILE_ERROR, G_KEY_FILE_ERROR_KEY_NOT_FOUND)) {
            // Key "nodes" is not defined, use default value instead.
        	cluster = strdup("127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003,127.0.0.1:7004,127.0.0.1:7005");
        } else {
            // Error, section "REDIS" is missing.
            return NULL;
        }
    }

    KV_Redis_Handle* handle = malloc(sizeof(KV_Redis_Handle));
    if(handle){
    	if((handle->ctx = redisClusterContextInit())){
    		if( redisClusterSetOptionAddNodes(handle->ctx, cluster) == 0 &&
    			redisClusterSetOptionRouteUseSlots(handle->ctx) == 0 	 &&
				redisClusterConnect2(handle->ctx) == 0						){
    		    free(cluster);
    		    return (KV_Handle)handle;
    		}

    		LogActivity(H3_ERROR_MSG, "HIREDIS_VIP - %s\n", handle->ctx->errstr);
    		redisClusterFree(handle->ctx);
    	}
    	free(handle);    }

    free(cluster);
    return NULL;
}

void KV_Redis_Free(KV_Handle handle) {
	KV_Redis_Handle* _handle = (KV_Redis_Handle*) handle;
	redisClusterFree(_handle->ctx);
    free(_handle);
    return;
}

static KV_Status ScanNode(redisContext* ctx, GTree* tree, KV_Key prefix){
	KV_Status status = KV_FAILURE;
    redisReply* reply = NULL;
    char cursor[12+1] = {'0', '\0'};

    do{
    	freeReplyObject(reply);
    	if((reply = redisCommand(ctx, "SCAN %s MATCH %s*", cursor, prefix))){
    		int i;

    		for(i=0; i<reply->element[1]->elements; i++){
    			g_tree_insert(tree, strdup(reply->element[1]->element[i]->str), GINT_TO_POINTER(reply->element[1]->element[i]->len));
    		}

    		strncpy(cursor, reply->element[0]->str, 12);
    	}

    }while(reply && strcmp(cursor, "0") != 0);

    if(reply){
    	freeReplyObject(reply);
    	status = KV_SUCCESS;
    }

    return status;
}

static KV_Status ScanCluster(redisClusterContext* cctx, GTree* tree, KV_Key prefix){
	KV_Status status = KV_SUCCESS;
    dictEntry* entry;

    if(cctx->nodes && dictSize(cctx->nodes) > 0) {

    	dictIterator* iter = dictGetIterator(cctx->nodes);
    	while((entry = dictNext(iter)) && status == KV_SUCCESS) {
    		struct cluster_node* node = dictGetEntryVal(entry);

    		if(node == NULL || node->con == NULL || !(node->con->flags&REDIS_CONNECTED))
    			continue;

    		status = ScanNode(node->con, tree, prefix);
    	}

    	dictReleaseIterator(iter);
    }

	return status;
}

typedef struct {
	KV_Key buffer;
	size_t remaining;
	uint8_t nTrim;
	uint32_t offset;
	uint32_t nMatchingKeys;
	uint32_t nRequiredKeys;
	KV_Status status;
}KV_Redis_UserData;

static gboolean KVTraverseCopy(gpointer key, gpointer value, gpointer _data){
	KV_Redis_UserData* data = (KV_Redis_UserData*) _data;

	if(data->offset)
		data->offset--;

	else if(data->nMatchingKeys < data->nRequiredKeys){
		char* entry = (char*)key;
		size_t entrySize = GPOINTER_TO_INT(value) - data->nTrim;
		if(data->remaining >= (entrySize + 1) ){
			memcpy(&data->buffer[KV_LIST_BUFFER_SIZE - data->remaining], &entry[data->nTrim], entrySize);
			data->remaining -= (entrySize+1);
			data->nMatchingKeys++;
		}
		else{
			data->status = KV_CONTINUE;
			free(key);
			return TRUE;
		}
	}
	else{
		data->status = KV_CONTINUE;
		free(key);
		return TRUE;
	}

	free(key);
	return FALSE;
}

static gboolean KVTraverseCount(gpointer key, gpointer value, gpointer _data){
	KV_Redis_UserData* data = (KV_Redis_UserData*) _data;

	free(key);

	if(data->offset)
		data->offset--;

	else if(data->nMatchingKeys < data->nRequiredKeys)
			data->nMatchingKeys++;

	else {
		data->status = KV_CONTINUE;
		return TRUE;
	}

	return FALSE;
}

KV_Status KV_Redis_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
	KV_Status status = KV_FAILURE;
    GTree* tree ;

    if( (tree = g_tree_new((GCompareFunc)strcmp)) ){

    	if( ScanCluster(storeHandle->ctx, tree, prefix) ){
    		KV_Redis_UserData data;
    		data.buffer = buffer;
    		data.remaining = KV_LIST_BUFFER_SIZE;
    		data.nTrim = nTrim;
    		data.offset = offset;
    		data.nMatchingKeys = 0;
    		data.nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    		data.status = KV_SUCCESS;

    		if(buffer){
    			memset(buffer, 0, KV_LIST_BUFFER_SIZE);
    			g_tree_foreach(tree, KVTraverseCopy, &data);
    		}
    		else
    			g_tree_foreach(tree, KVTraverseCount, &data);

    		status = data.status;
    		*nKeys = data.nMatchingKeys;
    	}

    	g_tree_destroy(tree);
    }

    return status;
}


//
// NOTE: The hiredis-vip client does not support cluster-wide scan operations thus we have to scan each node on out own
//
//KV_Status KV_Redis_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
//	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
//	KV_Status status = KV_SUCCESS;
//    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
//    uint32_t nMatchingKeys = 0;
//    size_t remaining = KV_LIST_BUFFER_SIZE;
//
//    redisReply* reply = NULL;
//    char* cursor = strdup("0");
//
//    if(buffer)
//    	memset(buffer, 0, KV_LIST_BUFFER_SIZE);
//
//    do{
//    	freeReplyObject(reply);
//    	if((reply = redisClusterCommand(storeHandle->ctx, "SCAN %s MATCH %s*", cursor, prefix))){
//    		int i;
//
//    		for(i=0; i<reply->element[1]->elements && status != KV_CONTINUE; i++){
//
//    			if(offset)
//    				offset--;
//
//    			else if( nMatchingKeys < nRequiredKeys ){
//
//    				// Copy the keys if a buffer is provided...
//    				if(buffer){
//    					size_t entrySize = reply->element[1]->element[i]->len - nTrim;
//    					if(remaining >= (entrySize + 1) ){
//    						memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &reply->element[1]->element[i]->str[nTrim], entrySize);
//    						remaining -= (entrySize+1);
//    						nMatchingKeys++;
//    					}
//    					else
//    						status = KV_CONTINUE;
//    				}
//
//    				// ... otherwise just count them.
//    				else
//    					nMatchingKeys++;
//    			}
//    			else
//    				status = KV_CONTINUE;
//    		}
//
//    		cursor = strdup(reply->element[0]->str);
//    	}
//
//    }while(reply && strcmp(cursor, "0") != 0 && status != KV_CONTINUE);
//
//    if(reply){
//    	freeReplyObject(reply);
//    	*nKeys = nMatchingKeys;
//    }
//    else
//    	status = KV_FAILURE;
//
//    return status;
//}


KV_Status KV_Redis_Exists(KV_Handle handle, KV_Key key) {
	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
    KV_Status status = KV_FAILURE;
    redisReply* reply;

    if((reply = redisClusterCommand(storeHandle->ctx, "EXISTS %s", key))){

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
		reply = redisClusterCommand(storeHandle->ctx, "GETRANGE %s %d %d", key, offset, size);
	else
		reply = redisClusterCommand(storeHandle->ctx, "GET %s", key);

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
		if((reply = redisClusterCommand(storeHandle->ctx, "SET %s %d NX", key, 0))){

			freeReplyObject(reply);
			if(reply->type == REDIS_REPLY_STRING ){

				//... and then populate it
				reply = redisClusterCommand(storeHandle->ctx, "SETRANGE %s %d %b", key, offset, value, size);
			}
			else{
				freeReplyObject(reply);
				return KV_KEY_EXIST;
			}
		}
	}
	else
		reply = redisClusterCommand(storeHandle->ctx, "SET %s %b NX", key, value, size);

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
		reply = redisClusterCommand(storeHandle->ctx, "SETRANGE %s %d %b", key, offset, value, size);
	else
		reply = redisClusterCommand(storeHandle->ctx, "SET %s %b", key, value, size);

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
    if((getReply = redisClusterCommand(storeHandle->ctx, "GET %s", src_key))){
    	if(getReply->type == REDIS_REPLY_STRING){
    		if((setReply = redisClusterCommand(storeHandle->ctx, "SET %s %b", dest_key, getReply->str, getReply->len))){
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

    if((reply = redisClusterCommand(storeHandle->ctx, "DEL %s", key))){

    	if(reply->integer == 0)
    		status = KV_KEY_NOT_EXIST;
    	else
    		status = KV_SUCCESS;

    	freeReplyObject(reply);
    }

    return status;
}


KV_Status KV_Redis_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
//	KV_Redis_Handle* storeHandle = (KV_Redis_Handle*) handle;
//	KV_Status status = KV_FAILURE;
//	redisReply* reply;
//
//	if((reply = redisClusterCommand(storeHandle->ctx, "RENAME %s %s", src_key, dest_key))){
//		switch(reply->type){
//			case REDIS_REPLY_STRING:
//				status = KV_SUCCESS;
//				break;
//
//			case REDIS_REPLY_ERROR:
//				status = KV_KEY_NOT_EXIST;
//				break;
//		}
//
//		freeReplyObject(reply);
//	}
//
//	return status;

	// NOTE: RENAME is not supported by the cluster-client
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

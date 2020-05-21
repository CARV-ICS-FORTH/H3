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

#include "kv_interface.h"
#include "util.h"

#include <kreon/kreon_rdma_client.h>

typedef struct {
    char* ip;
    int port;
//    GMutex lock;
}KV_Kreon_Handle;

void KV_Kreon_Free(KV_Handle _handle) {
	KV_Kreon_Handle* handle = (KV_Kreon_Handle*) _handle;
	krc_close();
//	g_mutex_clear(&handle->lock);
	free(handle->ip);
	free(handle);
    return;
}

KV_Handle KV_Kreon_Init(GKeyFile* cfgFile) {
    g_autoptr(GError) error = NULL;
    KV_Kreon_Handle* handle = malloc(sizeof(KV_Kreon_Handle));
    krc_ret_code status;

    handle->port = g_key_file_get_integer(cfgFile, "KREON", "port", &error);
    if(error){
	if( g_error_matches(error, G_KEY_FILE_ERROR, G_KEY_FILE_ERROR_KEY_NOT_FOUND)){
	    	handle->port = 2181;	// Default ZooKeeper port
    		g_clear_error(&error);
	 }
	 else{
	    	free(handle);
	    	return NULL;
	 }
     }

    handle->ip = g_key_file_get_string (cfgFile, "KREON", "zookeeper", &error);
    if(error){
       if( g_error_matches(error, G_KEY_FILE_ERROR, G_KEY_FILE_ERROR_KEY_NOT_FOUND)){
    	  handle->ip = strdup("127.0.0.1");
       }
       else{
    	 free(handle);
    	 return NULL;
       }
    }

//    g_mutex_init(&handle->lock);

    if((status = krc_init(handle->ip, handle->port)) != KRC_SUCCESS){
    	LogActivity(H3_ERROR_MSG, "Kreon - Failed to initialize (ip:%s  port:%d) Error:%d\n",handle->ip, handle->port, status);
    	KV_Kreon_Free(handle);
    	return NULL;
    }

    return (KV_Handle)handle;
}

KV_Status KV_Kreon_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
	KV_Status status = KV_SUCCESS;

    size_t remaining = KV_LIST_BUFFER_SIZE;
    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;
    krc_scannerp scanner;

    if(buffer)
    	memset(buffer, 0, KV_LIST_BUFFER_SIZE);

    if((scanner = krc_scan_init(16, KV_LIST_BUFFER_SIZE))){

    	krc_scan_set_prefix_filter(scanner, strlen(prefix), prefix);

    	char *key, *value;
    	size_t keySize, valueSize;
    	while(nMatchingKeys < nRequiredKeys && krc_scan_get_next(scanner, &key, &keySize, &value, &valueSize)){

    		LogActivity(H3_DEBUG_MSG, "keySize:%u key: '%*s'\n", keySize, keySize, key);

            if(offset)
                offset--;

            else {

            	// Copy the keys if a buffer is provided...
            	if(buffer){
                	size_t entrySize = keySize - nTrim;
                	if(remaining >= entrySize){
        				memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &key[nTrim], entrySize);
        				remaining -= (entrySize + 1); // Convert blob to string
        				nMatchingKeys++;
                	}
                	else
                		break;
            	}

            	// ... otherwise just count them.
            	else
            		nMatchingKeys++;
            }
    	}

    	krc_scan_close(scanner);
//    	free(stopMarker);
    	*nKeys = nMatchingKeys;
    }

	return status;
}

KV_Status KV_Kreon_Exists(KV_Handle _handle, KV_Key key) {
	if(krc_exists(strlen(key), key))
		return KV_KEY_EXIST;

	return KV_KEY_NOT_EXIST;
}

KV_Status KV_Kreon_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
	KV_Status status;

	switch(krc_get(strlen(key), key, (char**)value, (uint32_t*)size, (uint32_t)offset)){
		case KRC_SUCCESS: status = KV_SUCCESS; break;
		case KRC_KEY_NOT_FOUND: status = KV_KEY_NOT_EXIST; break;
		default: status = KV_FAILURE; break;
	}

	return status;
}

KV_Status KV_Kreon_Write(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {

	if(krc_put_with_offset(strlen(key), key, offset, size, value) == KRC_SUCCESS)
		return KV_SUCCESS;

	return KV_FAILURE;
}

KV_Status KV_Kreon_Delete(KV_Handle handle, KV_Key key) {

	KV_Status status;
	switch(krc_delete(strlen(key), key)){
		case KRC_SUCCESS: status = KV_SUCCESS; 				break;
		case KRC_KEY_NOT_FOUND: status = KV_KEY_NOT_EXIST; 	break;
		default: 				status = KV_FAILURE; 		break;

	}

	return status;
}

KV_Status KV_Kreon_Create(KV_Handle _handle, KV_Key key, KV_Value value, off_t offset, size_t size){
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

//	g_mutex_lock(&handle->lock);
	if( (status = KV_Kreon_Exists(handle, key)) == KV_KEY_NOT_EXIST){
		 status = KV_Kreon_Write(handle, key, value, offset, size);
	}
//	g_mutex_unlock(&handle->lock);

	return status;
}

KV_Status KV_Kreon_Copy(KV_Handle _handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

//	g_mutex_lock(&handle->lock);
	if((status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS){
		status = KV_Kreon_Write(handle, dest_key, value, 0, size);
	}
//	g_mutex_unlock(&handle->lock);

	return status;
}

KV_Status KV_Kreon_Move(KV_Handle _handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

//	g_mutex_lock(&handle->lock);
	if( (status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS &&
		(status = KV_Kreon_Write(handle, dest_key, value, 0, size)) == KV_SUCCESS		){
		status = KV_Kreon_Delete(handle, src_key);
	}
//	g_mutex_unlock(&handle->lock);

	return status;
}

KV_Status KV_Kreon_Sync(KV_Handle handle) {
    return KV_SUCCESS;
}

KV_Operations operationsKreon = {
    .init = KV_Kreon_Init,
    .free = KV_Kreon_Free,

    .metadata_read = KV_Kreon_Read,
    .metadata_write = KV_Kreon_Write,
    .metadata_create = KV_Kreon_Create,
    .metadata_delete = KV_Kreon_Delete,
    .metadata_move = KV_Kreon_Move,
    .metadata_exists = KV_Kreon_Exists,

    .list = KV_Kreon_List,
    .exists = KV_Kreon_Exists,
    .read = KV_Kreon_Read,
    .create = KV_Kreon_Create,
    .write = KV_Kreon_Write,
    .copy = KV_Kreon_Copy,
    .move = KV_Kreon_Move,
    .delete = KV_Kreon_Delete,
    .sync = KV_Kreon_Sync
};

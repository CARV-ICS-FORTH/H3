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

#include "kv_interface.h"
#include "common.h"
#include "util.h"
#include "url_parser.h"

#include <kreon/kreon_rdma_client.h>

#define KREON_COMPRESSION

#ifdef KREON_COMPRESSION

#include <zstd.h>

krc_ret_code krc_put_compressed(uint32_t key_size, void *key, uint32_t val_size, void *value) {
    void *compressed_value = malloc(H3_PART_SIZE);

    // The last argument to the function is the compression level, which can range from 1 (lowest) to 22 (highest).
    uint32_t compressed_value_len = ZSTD_compress(compressed_value, H3_PART_SIZE, value, val_size, -1);
    if (ZSTD_isError(compressed_value_len)) {
        LogActivity(H3_ERROR_MSG, "Failed to compress the value!");
        return KRC_FAILURE;
    }

    krc_ret_code result = krc_put(key_size, key, compressed_value_len, compressed_value);
    free(compressed_value);

    return result;
}

krc_ret_code krc_get_compressed(uint32_t key_size, char *key, char **buffer, uint32_t *size, uint32_t offset) {
    krc_ret_code result = krc_get(key_size, key, buffer, size, offset);
    if (result != KRC_SUCCESS) {
        return result;
    }

    void *decompressed_value = malloc(H3_PART_SIZE);

    uint32_t uncompressed_value_len = ZSTD_decompress(decompressed_value, H3_PART_SIZE, buffer, *size);
    if (ZSTD_isError(uncompressed_value_len)) {
        LogActivity(H3_ERROR_MSG, "Failed to decompress the value!");
        result = KRC_FAILURE;
    } else {
        result = KRC_SUCCESS;
    }

    free(*buffer);

    return result;
}

#else

krc_ret_code krc_put_compressed(uint32_t key_size, void *key, uint32_t val_size, void *value) {
    return krc_put(key_size, key, val_size, value);
}

krc_ret_code krc_get_compressed(uint32_t key_size, char *key, char **buffer, uint32_t *size, uint32_t offset) {
    return krc_get(key_size, key, buffer, size, offset);
}

#endif

typedef struct {
    char* ip;
    int port;
}KV_Kreon_Handle;

void KV_Kreon_Free(KV_Handle _handle) {
	KV_Kreon_Handle* handle = (KV_Kreon_Handle*) _handle;
	krc_close();
	free(handle->ip);
	free(handle);
    return;
}

KV_Handle KV_Kreon_Init(const char* storageUri) {
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
            port = 2181;
            LogActivity(H3_INFO_MSG, "WARNING: Unrecognized port in URI. Using default: 2181\n");
        }
    } else {
        port = 2181;
        LogActivity(H3_INFO_MSG, "WARNING: No port in URI. Using default: 2181\n");
    }
    parsed_url_free(url);

    KV_Kreon_Handle* handle = malloc(sizeof(KV_Kreon_Handle));
    krc_ret_code status;

    if (!handle)
        return NULL;

    handle->ip = host;
    handle->port = port;
    if((status = krc_init(handle->ip, handle->port)) != KRC_SUCCESS){
        LogActivity(H3_ERROR_MSG, "Kreon - Failed to initialize (ip:%s  port:%d) Error:%d\n",handle->ip, handle->port, status);
        KV_Kreon_Free(handle);
        return NULL;
    }

    return (KV_Handle)handle;
}

KV_Status KV_Kreon_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
	KV_Status status = KV_FAILURE;

    size_t remaining = KV_LIST_BUFFER_SIZE;
    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;
    krc_scannerp scanner;

    if((scanner = krc_scan_init(16, KV_LIST_BUFFER_SIZE))){

    	krc_scan_fetch_keys_only(scanner);
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
        				remaining -= entrySize;
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
    	*nKeys = nMatchingKeys;

    	status = KV_SUCCESS;
    }

	return status;
}

KV_Status KV_Kreon_Exists(KV_Handle _handle, KV_Key key) {
	if(krc_exists(strlen(key)+1, key))
		return KV_KEY_EXIST;

	return KV_KEY_NOT_EXIST;
}

KV_Status KV_Kreon_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
	KV_Status status;

	switch(krc_get_compressed(strlen(key)+1, key, (char**)value, (uint32_t*)size, (uint32_t)offset)){
		case KRC_SUCCESS: status = KV_SUCCESS; break;
		case KRC_KEY_NOT_FOUND: status = KV_KEY_NOT_EXIST; break;
		default: status = KV_FAILURE; break;
	}

	return status;
}

KV_Status KV_Kreon_Update(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {
    KV_Status status;

    KV_Value currentValue;
    size_t currentSize;
    KV_Value newValue;
    size_t newSize;
    int freeNewValue = 0;

    switch(krc_get_compressed(strlen(key)+1, key, (char**)&currentValue, (uint32_t*)&currentSize, 0)){
        case KRC_SUCCESS:
            if (offset + size <= currentSize) {
                newValue = currentValue;
                memcpy(currentValue + offset, value, size);
                newSize = currentSize;
            } else {
                newValue = (KV_Value)malloc(offset + size);
                freeNewValue = 1;
                memcpy(newValue, currentValue, currentSize);
                memcpy(newValue + offset, value, size);
                newSize = offset + size;
            }
            break;
        case KRC_KEY_NOT_FOUND:
            if (!offset) {
                newValue = value;
            } else {
                newValue = (KV_Value)calloc(1, offset + size);
                freeNewValue = 1;
                memcpy(newValue + offset, value, size);
            }
            newSize = offset + size;
            break;
        default:
            return KV_FAILURE;
            break;
    }

    // Convert key blob to string
    if(krc_put_compressed(strlen(key)+1, key, newSize, newValue) == KRC_SUCCESS) {
        status = KV_SUCCESS;
    } else {
        status = KV_FAILURE;
    }
    if (freeNewValue)
        free(newValue);

    return status;
}

KV_Status KV_Kreon_Write(KV_Handle handle, KV_Key key, KV_Value value, size_t size) {

	// Convert key blob to string
	if(krc_put_compressed(strlen(key)+1, key, size, value) == KRC_SUCCESS)
		return KV_SUCCESS;

	return KV_FAILURE;
}

KV_Status KV_Kreon_Delete(KV_Handle handle, KV_Key key) {

	KV_Status status;
	switch(krc_delete(strlen(key)+1, key)){
		case KRC_SUCCESS: status = KV_SUCCESS; 				break;
		case KRC_KEY_NOT_FOUND: status = KV_KEY_NOT_EXIST; 	break;
		default: 				status = KV_FAILURE; 		break;

	}

	return status;
}

KV_Status KV_Kreon_Create(KV_Handle _handle, KV_Key key, KV_Value value, size_t size){
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

	if( (status = KV_Kreon_Exists(handle, key)) == KV_KEY_NOT_EXIST){
		 status = KV_Kreon_Write(handle, key, value, size);
	}

	return status;
}

KV_Status KV_Kreon_Copy(KV_Handle _handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

	if((status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS){
		status = KV_Kreon_Write(handle, dest_key, value, size);
	}

	return status;
}

KV_Status KV_Kreon_Move(KV_Handle _handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;
	KV_Kreon_Handle* handle = (KV_Kreon_Handle *)_handle;

	if( (status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS &&
		(status = KV_Kreon_Write(handle, dest_key, value, size)) == KV_SUCCESS		){
		status = KV_Kreon_Delete(handle, src_key);
	}

	return status;
}

KV_Status KV_Kreon_Sync(KV_Handle handle) {
    return KV_SUCCESS;
}

KV_Operations operationsKreon = {
    .init = KV_Kreon_Init,
    .free = KV_Kreon_Free,
	.validate_key = NULL,

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
    .update = KV_Kreon_Update,
    .write = KV_Kreon_Write,
    .copy = KV_Kreon_Copy,
    .move = KV_Kreon_Move,
    .delete = KV_Kreon_Delete,
    .sync = KV_Kreon_Sync
};

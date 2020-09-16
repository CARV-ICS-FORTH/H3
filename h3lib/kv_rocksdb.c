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
#include <unistd.h>
#include <string.h>

#include <rocksdb/c.h>

#include "kv_interface.h"
#include "util.h"
#include "url_parser.h"

#define ROCKSDB_KEY_BATCH_SIZE 4096

typedef struct {
    char* path;
    rocksdb_t* db;
    rocksdb_options_t* options;
    rocksdb_readoptions_t* readoptions;
    rocksdb_writeoptions_t* writeoptions;
} KV_RocksDB_Handle;

KV_Handle KV_RocksDb_Init(const char* storageUri) {
    struct parsed_url *url = parse_url(storageUri);
    if (url == NULL) {
        LogActivity(H3_ERROR_MSG, "ERROR: Unrecognized storage URI\n");
        return NULL;
    }

    char *path;
    if (url->path != NULL) {
        path = malloc(strlen(url->path) + 2);
        path[0] = '/';
        strcpy(&(path[1]), url->path);
        LogActivity(H3_INFO_MSG, "INFO: Path in URI: %s\n", path);
    } else {
        path = strdup("/tmp/h3/rocksdb");
        LogActivity(H3_INFO_MSG, "WARNING: No path in URI. Using default: /tmp/h3\n");
    }
    parsed_url_free(url);

    // Prepare options.
    rocksdb_options_t *options = rocksdb_options_create();
    rocksdb_options_set_use_fsync(options, 0);


    // Parallelism options
    int cpus = (int)sysconf(_SC_NPROCESSORS_ONLN); // Get # of online cores
    rocksdb_options_increase_parallelism(options, cpus);
    rocksdb_options_set_max_background_flushes(options, 1);
    rocksdb_options_set_max_background_compactions(options, cpus - 1);
    rocksdb_options_set_max_subcompactions(options, 1); // Default is 1

    // General options
    long files = sysconf(_SC_OPEN_MAX); // Get the maximum number of files that a process can have open at any time.
    rocksdb_block_based_table_options_t* tableOptions = rocksdb_block_based_options_create();
    rocksdb_filterpolicy_t* filterPolicy = rocksdb_filterpolicy_create_bloom(10);
    rocksdb_block_based_options_set_filter_policy(tableOptions, filterPolicy);
    rocksdb_block_based_options_set_block_size(tableOptions, 1024 * 1024); // 1MB
    rocksdb_options_set_max_open_files(options, (int) (files * 0.9));


    // Flushing options
    rocksdb_options_set_write_buffer_size(options, 512 * __1MByte );
    rocksdb_options_set_max_write_buffer_number(options, 5);
    rocksdb_options_set_min_write_buffer_number_to_merge(options, 2);
    //rocksdb_options_optimize_level_style_compaction(options, 2 * __1GByte); // Overrides options write_buffer_size & max_write_buffer_number


    // Delete logs ASAP rather than archiving them
    rocksdb_options_set_WAL_ttl_seconds(options, 0);
	rocksdb_options_set_WAL_size_limit_MB(options, 0);

    rocksdb_readoptions_t* readoptions = rocksdb_readoptions_create();
    rocksdb_readoptions_set_verify_checksums(readoptions, 0); // Default is 0x00


    rocksdb_writeoptions_t* writeoptions = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(writeoptions, 0);	   // Default is 0x00
    rocksdb_writeoptions_disable_WAL(writeoptions, 1); // Bypass Write-Ahead-Log

    // Open database.
    char* err = NULL;
    rocksdb_options_set_compression(options, rocksdb_no_compression);
    rocksdb_options_set_compaction_style(options, rocksdb_level_compaction); //Default is 'level'
    rocksdb_options_set_create_if_missing(options, 1); // create the DB if it's not already present
    rocksdb_t *db = rocksdb_open(options, path, &err);
    if (err){
    	LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",err);
    	free(err);
        return NULL;
    }

    KV_RocksDB_Handle* handle = malloc(sizeof(KV_RocksDB_Handle));
    handle->path = path;
    handle->db = db;
    handle->readoptions = readoptions;
    handle->writeoptions = writeoptions;
//    g_mutex_init(&handle->lock);
    return (KV_Handle)handle;
}

void KV_RocksDb_Free(KV_Handle handle) {
    KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle*)handle;

    rocksdb_writeoptions_destroy(storeHandle->writeoptions);
    rocksdb_readoptions_destroy(storeHandle->readoptions);
//    rocksdb_options_destroy(storeHandle->options);			// TODO <-- sometimes we crash here !!!
    rocksdb_close(storeHandle->db);

    free(storeHandle->path);
    free(storeHandle);
}

KV_Status KV_RocksDb_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys) {
	KV_Status status = KV_SUCCESS;
    KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle *)handle;

    size_t remaining = KV_LIST_BUFFER_SIZE;
    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;

    rocksdb_iterator_t* iter = rocksdb_create_iterator(storeHandle->db, storeHandle->readoptions);
    if(!iter){
    	return KV_FAILURE;
    }

    size_t prefixLen = strlen(prefix);
    rocksdb_iter_seek(iter, prefix, prefixLen);

    while(rocksdb_iter_valid(iter) && status != KV_CONTINUE){
    	size_t keySize;
    	const char* key = rocksdb_iter_key(iter, &keySize);

    	// It seems the prefix doesn't work the way we expect it, that is it returns
    	// all the entries rather than those matching the prefix.
    	if(strncmp(key, prefix, prefixLen) == 0){

			if(offset)
				offset--;
			else if( nMatchingKeys < nRequiredKeys ){

				// Copy the keys if a buffer is provided...
				if(buffer){
					size_t entrySize = keySize - nTrim;
					if(remaining >= entrySize ){
						memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &key[nTrim], entrySize);
						remaining -= entrySize; // Convert blob to string
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

    	rocksdb_iter_next(iter);
    }

    char* error = NULL;
    rocksdb_iter_get_error(iter, &error);
    if(error){
    	LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
    	free(error);
    	status = KV_FAILURE;
    }

    rocksdb_iter_destroy(iter);

    *nKeys = nMatchingKeys;

    return status;
}

KV_Status KV_RocksDb_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
	KV_Status status = KV_FAILURE;
	char *segment, *error = NULL;
	KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle*) handle;

	size_t bufferSize, segmentSize;
	char* buffer = rocksdb_get(storeHandle->db, storeHandle->readoptions, key, strlen(key)+1, &bufferSize, &error);

	if(error){
		LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
		free(error);
		return KV_FAILURE;
	}

	if(!buffer)
		return KV_KEY_NOT_EXIST;

	if(offset > bufferSize){
		free(buffer);
		*size = 0;
		return KV_SUCCESS;
	}

	if(*value == NULL){
		if(!offset){
			*value = (KV_Value)buffer;
			*size = bufferSize;
			return KV_SUCCESS;
		}

		segmentSize = bufferSize - offset;
		if((segment = malloc(segmentSize))){
			memcpy(segment, buffer + offset, segmentSize);
			*value = (KV_Value)segment;
			*size = segmentSize;
			status = KV_SUCCESS;
		}
	}
	else{
		segmentSize = min(bufferSize - offset, *size);
		memcpy(*value, buffer + offset, segmentSize);
		*size = segmentSize;
		status = KV_SUCCESS;

	}

	free(buffer);
	return status;
}

KV_Status KV_RocksDb_Update(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {

    // XXX Do this with a merge operation...
	//     i.e. rocksdb_mergeoperator_create()
	//          rocksdb_options_set_merge_operator() <-- Call it before rocksdb_open()
	//          rocksdb_merge()
	//          rocksdb_mergeoperator_destroy()
	// Note: We can only have a single merge-operator, thus if we need more flavors we have to prepend the key
	//       with the flavor, i.e. WRITE:, EXISTS, COPY:, RENAME:, etc

    char* error = NULL;
    KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle *)handle;

    // Retrieve previous data if any
    size_t bufferSize;
    char* buffer = rocksdb_get(storeHandle->db, storeHandle->readoptions, key, strlen(key)+1, &bufferSize, &error);
	if(error){
		LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
		free(error);
		return KV_FAILURE;
	}

	// Brand new KV pair
	if(!buffer){
		if(offset){
			if((value = realloc(value, offset + size))){
				memmove(value + offset, value, size);
				memset(value, 0, offset);
			}
			else
				return KV_FAILURE;
		}

		rocksdb_put(storeHandle->db, storeHandle->writeoptions, key, strlen(key)+1, (char*)value, offset + size, &error);
        if (error){
        	LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
        	free(error);
            return KV_FAILURE;
        }
	}

	// Patch buffer with user-data
	else {
		char* patchedBuffer = buffer;
		if((offset + size) > bufferSize){
			if( (patchedBuffer = realloc(buffer, offset + size)) ){
			memset(patchedBuffer+bufferSize, 0, (offset + size) - bufferSize);
			}
			else
				return KV_FAILURE;
		}

		memcpy(patchedBuffer+offset, value, size);

		rocksdb_put(storeHandle->db, storeHandle->writeoptions, key, strlen(key)+1, patchedBuffer, max((offset + size),bufferSize), &error);
		free(patchedBuffer);
		if (error){
			LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
			free(error);
			return KV_FAILURE;
		}
	}

    return KV_SUCCESS;
}

KV_Status KV_RocksDb_Write(KV_Handle handle, KV_Key key, KV_Value value, size_t size) {
    char* error = NULL;
    KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle *)handle;

    rocksdb_put(storeHandle->db, storeHandle->writeoptions, key, strlen(key)+1, (char*)value, size, &error);
    if (error){
        LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
        free(error);
        return KV_FAILURE;
    }

    return KV_SUCCESS;
}

KV_Status KV_RocksDb_Delete(KV_Handle handle, KV_Key key) {
    KV_RocksDB_Handle* storeHandle = (KV_RocksDB_Handle *)handle;

    char* error = NULL;
    rocksdb_delete(storeHandle->db, storeHandle->writeoptions, key, strlen(key)+1, &error);
    if (error){
    	LogActivity(H3_ERROR_MSG, "RocksDB - %s\n",error);
    	free(error);
        return KV_FAILURE;
    }

    return KV_SUCCESS;
}

KV_Status KV_RocksDb_Exists(KV_Handle handle, KV_Key key) {
	KV_Status status;
    KV_Value dummy = NULL;
    size_t size = 0x00;

    if( (status = KV_RocksDb_Read(handle, key, 0, &dummy, &size)) == KV_SUCCESS){
    	free(dummy);
    	status = KV_KEY_EXIST;
    }

    return status;
}

KV_Status KV_RocksDb_Create(KV_Handle handle, KV_Key key, KV_Value value, size_t size) {
	KV_Status status;

	if( (status = KV_RocksDb_Exists(handle, key)) == KV_KEY_NOT_EXIST){
		 status = KV_RocksDb_Write(handle, key, value, size);
	}

	return status;
}

KV_Status KV_RocksDb_Copy(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;

	if((status = KV_RocksDb_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS){
		status = KV_RocksDb_Write(handle, dest_key, value, size);
	}

	return status;
}

KV_Status KV_RocksDb_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
	KV_Status status;

	if( (status = KV_RocksDb_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS &&
		(status = KV_RocksDb_Write(handle, dest_key, value, size)) == KV_SUCCESS		){
		status = KV_RocksDb_Delete(handle, src_key);
	}

	return status;
}

KV_Status KV_RocksDb_Sync(KV_Handle handle) {
    return KV_SUCCESS;
}

KV_Operations operationsRocksDB = {
	.init = KV_RocksDb_Init,
	.free = KV_RocksDb_Free,
	.validate_key = NULL,

	.metadata_read = KV_RocksDb_Read,
	.metadata_write = KV_RocksDb_Write,
	.metadata_create = KV_RocksDb_Create,
	.metadata_delete = KV_RocksDb_Delete,
	.metadata_move = KV_RocksDb_Move,
	.metadata_exists = KV_RocksDb_Exists,

	.list = KV_RocksDb_List,
	.exists = KV_RocksDb_Exists,
	.read = KV_RocksDb_Read,
	.create = KV_RocksDb_Create,
    .update = KV_RocksDb_Update,
	.write = KV_RocksDb_Write,
	.copy = KV_RocksDb_Copy,
	.move = KV_RocksDb_Move,
	.delete = KV_RocksDb_Delete,
	.sync = KV_RocksDb_Sync
};

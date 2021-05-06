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
#include <alloca.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <ftw.h>
#include <regex.h>
#include <ctype.h>

#include "common.h"
#include "kv_interface.h"

#include "util.h"
#include "url_parser.h"

#define KV_FS_DIRECTORY_CHAR	0x7F	// In place of last slash to turn directory object into file object

typedef struct {
    char * metadata_root;
    char* root;
    int metadata_root_path_len;
    int root_path_len;
}KV_Filesystem_Handle;

static void StripSlashes(char* path){
    int size = strlen(path);
    for(int i=0; i<size - 1; i++) {
        if(path[i] == '/' && path[i+1] == '/') {
            for(int j=i; j<size; j++) {
                    path[j] = path[j + 1];
            }
        }
    }
    for(int i=size-1; size>0; size--) {
        if(path[i] == '/') {
            path[i] = '\0';
        }
    }
}

static int DoMkdir(const char *path, mode_t mode) {
    struct stat st;
    int error = 0;

    if (stat(path, &st) != 0) {
        /* Directory does not exist. EEXIST for race condition */
        if (mkdir(path, mode) != 0 && errno != EEXIST)
            error = 1;
    }
    else if (!S_ISDIR(st.st_mode)) {
        errno = ENOTDIR;
        error = 1;
    }

    return error;
}

static int MakePath(const char* fullKey, mode_t mode) {
    char *pp, *sp, *copypath = strdup(fullKey);
    int error = 0;

    // The key is expected to end with a filename or '/'
    // otherwise the last sub-dir will not be created

    pp = copypath;
    while(!error && (sp = strchr(pp, '/'))){
        if (sp != pp) {
            *sp = '\0';
            error = DoMkdir(copypath, mode);
            *sp = '/';
        }
        pp = sp + 1;
    }
    free(copypath);

    return !error;
}

static char* GetFullKey(KV_Filesystem_Handle* handle, KV_Key key){
	int size = 0;
	char* fullKey = NULL;
	if(handle && key && (size = asprintf(&fullKey, "%s/%s", handle->root, key)) > 0){
		if(fullKey[size-1] == '/'){
			fullKey[size-1] = KV_FS_DIRECTORY_CHAR;
		}
	}
	return fullKey;
}

static char* GetFullPrefix(KV_Filesystem_Handle* handle, KV_Key key){
	char* fullKey = NULL;
	if(handle && key){
		asprintf(&fullKey, "%s/%s", handle->root, key);
	}
	return fullKey;
}

static KV_Status Write(int fd, KV_Value value, off_t offset, size_t size){
    KV_Status status = KV_FAILURE;

    if(size){
        if(value){
            if(lseek(fd, offset, SEEK_SET) != offset){
                LogActivity(H3_ERROR_MSG, "Error create/write seeking in offset %" PRIu64 "\n",offset);
            }
            else{
                // Use an attempt counter to allow for legitimate partial writes
                int attempts = 2;
                size_t writen, remaining = size;
                while(--attempts && remaining){
                    KV_Value buffer = &value[size - remaining];
                    if((writen = write(fd, buffer, remaining)) == -1){
                        break;
                    }
                    remaining -= writen;
                }

                if(!remaining)
                    status = KV_SUCCESS;
            }
        }
    }
    else
        status = KV_SUCCESS;

    close(fd);

    return  status;
}



KV_Handle KV_FS_Init(const char* storageUri) {
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
        path = strdup("/tmp/h3");
        LogActivity(H3_INFO_MSG, "WARNING: No path in URI. Using default: /tmp/h3\n");
    }
    parsed_url_free(url);

    KV_Filesystem_Handle* handle = malloc(sizeof(KV_Filesystem_Handle));
    handle->root = path;
    StripSlashes(handle->root);
    handle->root_path_len = strlen(handle->root);

    return (KV_Handle)handle;
}

void KV_FS_Free(KV_Handle handle) {
    KV_Filesystem_Handle* iHandle = (KV_Filesystem_Handle*) handle;
    free(iHandle->root);
    free(iHandle);
    return;
}

KV_Status KV_FS_ValidateKey(KV_Key key){
	KV_Status status = KV_INVALID_KEY;
	regex_t regex;

	// https://regex101.com/

	//	regcomp(&regex, "(^/)|([^/_+@%0-9a-zA-Z.-])|(/{2,})|(/$)|(%.)", REG_EXTENDED)

	// Cannot start with '/'
	// Cannot contain '//'
	// Cannot end with ASCII x7F i.e. DEL
	if( regcomp(&regex, "(^/)|(/{2,})|(\x7F$)", REG_EXTENDED) == REG_NOERROR){
		if(regexec(&regex, key, 0, NULL, 0) == REG_NOMATCH){
			status = KV_SUCCESS;
		}

		regfree(&regex);
	}
	return status;
}


KV_Status KV_FS_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullPrefix = GetFullPrefix(storeHandle, prefix);
    int status = KV_FAILURE;

    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;
    size_t prefixLen = strlen(fullPrefix);
    size_t rootLen = strlen(storeHandle->root) + nTrim + 1;
    size_t remaining = KV_LIST_BUFFER_SIZE;

    int CopyDirEntry(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) {

    	if(S_ISREG(sb->st_mode)){
    		size_t rawSize = strlen(fpath);
    		int isFakeDir = iscntrl(fpath[rawSize-1]);

    		if(strncmp(fpath, fullPrefix, prefixLen) == 0 || (isFakeDir && fullPrefix[prefixLen-1] == '/' && strncmp(fpath, fullPrefix, prefixLen -1) == 0 )){
    			LogActivity(H3_DEBUG_MSG, "'%s'\n", fpath);
                if(offset)
                    offset--;

                else if( nMatchingKeys < nRequiredKeys ){
                	size_t entrySize = rawSize - rootLen;

                    if(remaining >= (entrySize + 1)) {
                        memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &fpath[rootLen], entrySize);

                        // Replace the directory marker with '/'
                        if(isFakeDir){
                        	buffer[KV_LIST_BUFFER_SIZE - remaining + entrySize - 1] = '/';
                        }

                        remaining -= (entrySize+1);
                        nMatchingKeys++;
                    }
                    else
                        return KV_CONTINUE;
                }
                else
                    return KV_CONTINUE;
    		}
    	}

        return 0;
    }


    int CountDirEntry(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) {
    	if(S_ISREG(sb->st_mode)){
    		size_t rawSize = strlen(fpath);
    		int isFakeDir = iscntrl(fpath[rawSize-1]);

    		if(strncmp(fpath, fullPrefix, prefixLen) == 0 || (isFakeDir && fullPrefix[prefixLen-1] == '/')){
                if(offset)
                    offset--;
                else if (nMatchingKeys < nRequiredKeys)
                    nMatchingKeys++;
                else
                    return KV_CONTINUE;

    		}
    	}

        return 0;
    }

    if(buffer){
    	memset(buffer, 0, KV_LIST_BUFFER_SIZE);
        status = nftw(storeHandle->root, CopyDirEntry, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS);
    }
    else
        status = nftw(storeHandle->root, CountDirEntry, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS);

    *nKeys = nMatchingKeys;

    if(status == 0)
        status = KV_SUCCESS;

    else if (status < -0){
    	if (errno == ENAMETOOLONG)
    		status = KV_KEY_TOO_LONG;
    	else{
    		status = KV_FAILURE;
    		LogActivity(H3_ERROR_MSG, "Listing from key %s failed - %s\n",fullPrefix, strerror(errno));
    	}
    }

    return (KV_Status)status;
}


KV_Status KV_FS_Exists(KV_Handle handle, KV_Key key) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullKey = GetFullKey(storeHandle, key);
    KV_Status status = KV_FAILURE;

    if(!fullKey){
        return KV_FAILURE;
    }

    if(access(fullKey, F_OK) == 0)
        status = KV_KEY_EXIST;
    else if(errno == ENOENT)
        status = KV_KEY_NOT_EXIST;
    else if(errno == ENAMETOOLONG)
        status = KV_KEY_TOO_LONG;
    else
    	LogActivity(H3_ERROR_MSG, "Checking key %s failed - %s\n",fullKey, strerror(errno));

    free(fullKey);
    return status;
}

KV_Status KV_FS_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullKey = GetFullKey(storeHandle, key);
    KV_Status status = KV_FAILURE;
    struct stat st;
    char freeOnError = 0;

    if(!fullKey){
        return KV_FAILURE;
    }

    if(stat(fullKey, &st)!= -1 && S_ISREG(st.st_mode)){
    	int fd;

		if( (fd = open(fullKey, O_RDONLY)) != -1){

			// Check if we need to allocate the buffer
			if(*value == NULL){
				*size = st.st_size - offset;
				*value = malloc(*size - offset);
				freeOnError = 1;
			}

			// At this point we MUST have a buffer
			if( *value ){
				// Read the data
				if(lseek(fd, offset, SEEK_SET) != offset){
					LogActivity(H3_ERROR_MSG, "Error read seeking in offset %" PRIu64 "\n",offset);
				}
				else if((*size = read(fd, *value, *size)) != -1){
					status = KV_SUCCESS;
				}
			}
			close(fd);
		}
    }

    if(status != KV_SUCCESS ){

        if(freeOnError && *value)
        	free(*value);

    	switch(errno){
			case ENAMETOOLONG:
				status = KV_KEY_TOO_LONG; break;

			case ENOENT:
			case EISDIR:
				status = KV_KEY_NOT_EXIST; break;

//			case ENOTDIR:
//				if(strchr(key, '/')) status = KV_KEY_NOT_EXIST;
//				break;

			default:{
//				status = KV_FAILURE;
				LogActivity(H3_ERROR_MSG, "Reading from key %s failed - %s\n",fullKey, strerror(errno));
			}
    	}
    }

    free(fullKey);
    return status;
}

KV_Status KV_FS_Create(KV_Handle handle, KV_Key key, KV_Value value, size_t size){
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullKey = GetFullKey(storeHandle, key);
    KV_Status status = KV_FAILURE;
    int fd;

    if(!fullKey){
        return KV_FAILURE;
    }

    MakePath(fullKey, S_IRWXU | S_IRWXG | S_IRWXO);

    if( (fd = open(fullKey,O_CREAT|O_EXCL|O_WRONLY,0666)) != -1){
        return Write(fd, value, 0, size);
    }
    else if( errno == EEXIST ){
        status =  KV_KEY_EXIST;
    }
    else if(errno == ENAMETOOLONG){
    	status = KV_KEY_TOO_LONG;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Creating key %s failed - %s\n",fullKey, strerror(errno));
    }

    free(fullKey);
    return status;
}

KV_Status KV_FS_Update(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullKey = GetFullKey(storeHandle, key);
    KV_Status status = KV_FAILURE;
    int fd;

    if(!fullKey){
        return KV_FAILURE;
    }

    MakePath(fullKey, S_IRWXU | S_IRWXG | S_IRWXO);

    if( (fd = open(fullKey,O_CREAT|O_WRONLY,0666)) != -1){
        return Write(fd, value, offset, size);
    }
    else if( errno == EEXIST ){
        status =  KV_KEY_EXIST;
    }
    else if(errno == ENAMETOOLONG){
    	status = KV_KEY_TOO_LONG;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Writing key %s failed - %s\n",key, strerror(errno));
    }

    free(fullKey);
    return status;
}

KV_Status KV_FS_Write(KV_Handle handle, KV_Key key, KV_Value value, size_t size) {
    return KV_FS_Update(handle, key, value, 0, size);
}

KV_Status KV_FS_Copy(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char *srcFullKey, *dstFullKey;
    KV_Status status = KV_FAILURE;
    int srcFd = -1, dstFd = -1;

    if(!(srcFullKey = GetFullKey(storeHandle, src_key))){
        return KV_FAILURE;
    }

    if(!(dstFullKey = GetFullKey(storeHandle, dest_key))){
        free(srcFullKey);
        return KV_FAILURE;
    }

    MakePath(dstFullKey, S_IRWXU | S_IRWXG | S_IRWXO);

    if( (srcFd = open(srcFullKey, O_RDONLY)) != -1 ){
        if( (dstFd = open(dstFullKey, O_CREAT|O_WRONLY|O_TRUNC, 0666)) != -1){
            ssize_t readSize, writeSize;
            char buffer[4096]; // 4K

            while((readSize = read(srcFd, buffer, 4096)) > 0 && (writeSize = write(dstFd, buffer, readSize)) > 0 && readSize == writeSize);
            if(readSize == 0){
                status = KV_SUCCESS;
            }
            close(dstFd);
        }
        else if(errno == ENAMETOOLONG){
        	status = KV_KEY_TOO_LONG;
        }
        close(srcFd);
    }
    else if(errno == ENOENT){
        status = KV_KEY_NOT_EXIST;
    }
    else if(errno == ENAMETOOLONG){
    	status = KV_KEY_TOO_LONG;
    }
    else{
        LogActivity(H3_ERROR_MSG, "Copying key %s to %s failed - %s\n",src_key, dest_key, strerror(errno));
    }

    free(srcFullKey);
    free(dstFullKey);

    return status;
}

KV_Status KV_FS_Delete(KV_Handle handle, KV_Key key) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char* fullKey = GetFullKey(storeHandle, key);
    KV_Status status = KV_FAILURE;

    if(fullKey){
    	if(remove(fullKey) == 0){
    		status = KV_SUCCESS;
    	}
    	else if(errno == ENOENT){
    		status = KV_KEY_NOT_EXIST;
    	}
    	else if(errno == ENAMETOOLONG){
    		status = KV_KEY_TOO_LONG;
    	}
    	else{
    		LogActivity(H3_ERROR_MSG, "Deleting key %s failed - %s\n",key, strerror(errno));
    	}

    	free(fullKey);
    }

    return status;
}


KV_Status KV_FS_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    KV_Filesystem_Handle* storeHandle = (KV_Filesystem_Handle*) handle;
    char *srcFullKey, *dstFullKey;
    KV_Status status = KV_FAILURE;

    if(!(srcFullKey = GetFullKey(storeHandle, src_key))){
        return KV_FAILURE;
    }

    if(!(dstFullKey = GetFullKey(storeHandle, dest_key))){
        free(srcFullKey);
        return KV_FAILURE;
    }

    MakePath(dstFullKey, S_IRWXU | S_IRWXG | S_IRWXO);

    if( rename(srcFullKey, dstFullKey) != -1){
        status = KV_SUCCESS;
    }
    else if(errno == ENOENT){
        status = KV_KEY_NOT_EXIST;
    }
	else if(errno == ENAMETOOLONG){
		status = KV_KEY_TOO_LONG;
	}
    else {
        LogActivity(H3_ERROR_MSG, "Moving key %s to %s failed - %s\n",src_key, dest_key, strerror(errno));
    }

    free(srcFullKey);
    free(dstFullKey);

    return status;
}


KV_Status KV_FS_Sync(KV_Handle handle) {
    return KV_FAILURE;
}


KV_Operations operationsFilesystem = {
    .init = KV_FS_Init,
    .free = KV_FS_Free,
	.validate_key = KV_FS_ValidateKey,

    .metadata_read = KV_FS_Read,
    .metadata_write = KV_FS_Write,
    .metadata_create = KV_FS_Create,
    .metadata_delete = KV_FS_Delete,
    .metadata_move = KV_FS_Move,
    .metadata_exists = KV_FS_Exists,

    .list = KV_FS_List,
    .exists = KV_FS_Exists,
    .read = KV_FS_Read,
    .create = KV_FS_Create,
    .update = KV_FS_Update,
    .write = KV_FS_Write,
    .copy = KV_FS_Copy,
    .move = KV_FS_Move,
    .delete = KV_FS_Delete,
    .sync = KV_FS_Sync
};

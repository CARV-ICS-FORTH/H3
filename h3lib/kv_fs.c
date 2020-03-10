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
#include <glib.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <ftw.h>

#include "kv_interface.h"

#include "util.h"

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
   char* fullKey = NULL;
   if(handle && key)
       asprintf(&fullKey, "%s/%s", handle->root, key);
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



KV_Handle KV_FS_Init(GKeyFile* cfgFile) {

    g_autoptr(GError) error = NULL;
    KV_Filesystem_Handle* handle = malloc(sizeof(KV_Filesystem_Handle));

    handle->root = g_key_file_get_string (cfgFile, "FILESYSTEM", "root", &error);
    if(handle->root == NULL){
        // Key 'root' is not defined, or section 'FILESYSTEM' is missing, use default value instead
        handle->root = strdup("/tmp/h3");
    }
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


KV_Status KV_FS_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* fullPrefix = GetFullKey(fs_handle, prefix);
    int status = KV_FAILURE;

    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;
    size_t prefixLen = strlen(fullPrefix);
    size_t rootLen = strlen(fs_handle->root) + nTrim + 1;
    size_t remaining = KV_LIST_BUFFER_SIZE;

    int CopyDirEntry(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) {
        if(strncmp(fpath, fullPrefix, prefixLen) == 0){
            LogActivity(H3_DEBUG_MSG, "%s\n", fpath);
            if(offset)
                offset--;
            else if( nMatchingKeys < nRequiredKeys ){
                size_t entrySize = strlen(fpath) + 1 - rootLen;
                if(remaining >= entrySize) {
//                    LogActivity(H3_DEBUG_MSG, "%s\n", &fpath[rootLen]);
                    memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &fpath[rootLen], entrySize);
                    remaining -= entrySize;
                    nMatchingKeys++;
                }
                else
                    return KV_CONTINUE;
            }
            else
                return KV_CONTINUE;
        }

        return 0;
    }


    int CountDirEntry(const char* fpath, const struct stat* sb, int typeflag, struct FTW* ftwbuf) {
        if(strncmp(fpath, fullPrefix, prefixLen) == 0){
            if(offset)
                offset--;
            else if (nMatchingKeys < nRequiredKeys)
                nMatchingKeys++;
            else
                return KV_CONTINUE;
        }

        return 0;
    }

    if(buffer)
        status = nftw(fs_handle->root, CopyDirEntry, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS);
    else
        status = nftw(fs_handle->root, CountDirEntry, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS);

    *nKeys = nMatchingKeys;

    if(status == 0)
        status = KV_SUCCESS;
    else if (status < -0)
        status = KV_FAILURE;



    return (KV_Status)status;
}


KV_Status KV_FS_Exists(KV_Handle handle, KV_Key key) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* full_key = GetFullKey(fs_handle, key);
    KV_Status status = KV_FAILURE;

    if(!full_key){
        return KV_FAILURE;
    }

    if(access(full_key, F_OK) == 0)
        status = KV_KEY_EXIST;
    else if(errno == ENOENT)
        status = KV_KEY_NOT_EXIST;

    free(full_key);
    return status;
}

KV_Status KV_FS_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* full_key = GetFullKey(fs_handle, key);
    KV_Status status = KV_FAILURE;
    char freeOnError = 0;
    int fd;

    if(!full_key){
        return KV_FAILURE;
    }

    // Check if we need to allocate the buffer
    if(*value == NULL){
        // Retrieve size if not set
        if( *size == 0){
            struct stat stats;
            if(stat(full_key, &stats) == 0){
                *size = stats.st_size;
            }
        }

        // Allocate buffer
        *value = malloc(*size);
        freeOnError = 1;
    }

    // At this point we MUST have a buffer
    if(*value == NULL){
        return KV_FAILURE;
    }

    // Read the data
    if( (fd = open(full_key, O_RDONLY)) != -1){
        if(lseek(fd, offset, SEEK_SET) != offset){
            LogActivity(H3_ERROR_MSG, "Error read seeking in offset %" PRIu64 "\n",offset);
        }
        else if((*size = read(fd, *value, *size)) != -1){
            status = KV_SUCCESS;
        }
        close(fd);
    }
    else if( errno == ENOENT || (errno == ENOTDIR && strchr(key, '/'))){
        status =  KV_KEY_NOT_EXIST;
        if(freeOnError)
            free(*value);
    }

    if(errno == ENAMETOOLONG){
    	return KV_NAME_TO_LONG;
    }

    free(full_key);
    return status;
}

KV_Status KV_FS_Create(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size){
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* full_key = GetFullKey(fs_handle, key);
    KV_Status status = KV_FAILURE;
    int fd;

    if(!full_key){
        return KV_FAILURE;
    }

    MakePath(full_key, S_IRWXU | S_IRWXG | S_IRWXO);

    if( (fd = open(full_key,O_CREAT|O_EXCL|O_WRONLY,0666)) != -1){
        return Write(fd, value, offset, size);
    }
    else if( errno == EEXIST ){
        status =  KV_KEY_EXIST;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Creating key %s failed - %s\n",full_key, strerror(errno));
    }

    free(full_key);
    return status;
}

KV_Status KV_FS_Write(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* full_key = GetFullKey(fs_handle, key);
    KV_Status status = KV_FAILURE;
    int fd;

    if(!full_key){
        return KV_FAILURE;
    }

    MakePath(full_key, S_IRWXU | S_IRWXG | S_IRWXO);

    if( (fd = open(full_key,O_CREAT|O_WRONLY,0666)) != -1){
        return Write(fd, value, offset, size);
    }
    else if( errno == EEXIST ){
        status =  KV_KEY_EXIST;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Writing key %s failed - %s\n",key, strerror(errno));
    }

    free(full_key);
    return status;
}

KV_Status KV_FS_Copy(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char *srcFullKey, *dstFullKey;
    KV_Status status = KV_FAILURE;
    int srcFd = -1, dstFd = -1;

    if(!(srcFullKey = GetFullKey(fs_handle, src_key))){
        return KV_FAILURE;
    }

    if(!(dstFullKey = GetFullKey(fs_handle, dest_key))){
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
        close(srcFd);
    }
    else if(errno == ENOENT){
        status = KV_KEY_NOT_EXIST;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Copying key %s to %s failed - %s\n",src_key, dest_key, strerror(errno));
    }

    free(srcFullKey);
    free(dstFullKey);

    return status;
}

KV_Status KV_FS_Delete(KV_Handle handle, KV_Key key) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char* full_key = GetFullKey(fs_handle, key);
    KV_Status status = KV_FAILURE;

    // TODO - Delete empty directories

    if(full_key && remove(full_key) == 0){
        status = KV_SUCCESS;
    }
    else if(errno == ENOENT){
        status = KV_KEY_NOT_EXIST;
    }
    else {
        LogActivity(H3_ERROR_MSG, "Deleting key %s failed - %s\n",key, strerror(errno));
    }

    free(full_key);
    return status;
}


KV_Status KV_FS_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    KV_Filesystem_Handle* fs_handle = (KV_Filesystem_Handle*) handle;
    char *srcFullKey, *dstFullKey;
    KV_Status status = KV_FAILURE;

    if(!(srcFullKey = GetFullKey(fs_handle, src_key))){
        return KV_FAILURE;
    }

    if(!(dstFullKey = GetFullKey(fs_handle, dest_key))){
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
    .write = KV_FS_Write,
    .copy = KV_FS_Copy,
    .move = KV_FS_Move,
    .delete = KV_FS_Delete,
    .sync = KV_FS_Sync
};

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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "kv_interface.h"
#include "common.h"
#include "util.h"
#include "url_parser.h"

#include <kreon.h>

void KV_Kreon_Free(KV_Handle handle) {
    klc_sync(handle);
    // free(handle->volume_name);
    return;
}

KV_Handle KV_Kreon_Init(const char* storageUri) {
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
        path = strdup("/tmp/h3/kreon.dat");
        LogActivity(H3_INFO_MSG, "WARNING: No path in URI. Using default: /tmp/h3/kreon.dat\n");
    }
    parsed_url_free(url);

    int64_t size;
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        LogActivity(H3_ERROR_MSG, "ERROR: Can not open %s\n", path);
        free(path);
        return NULL;
    }
    if (ioctl(fd, BLKGETSIZE64, &size) == -1) {
        // Maybe this is a file
        size = lseek(fd, 0, SEEK_END);
        if (size == -1) {
            LogActivity(H3_ERROR_MSG, "ERROR: Can not determine size of %s\n", path);
            free(path);
            return NULL;
        }
    }
    close(fd);

    klc_db_options db_options;
    db_options.volume_name = path;
    db_options.db_name = path;
    db_options.volume_start = 0;
    db_options.volume_size = size;
    db_options.create_flag = KLC_CREATE_DB;

    klc_handle handle = klc_open(&db_options);
    if (!handle) {
        LogActivity(H3_ERROR_MSG, "ERROR: Failed to initialize Kreon\n");
        free(path);
        return NULL;
    }

    return handle;
}

KV_Status KV_Kreon_List(KV_Handle handle, KV_Key prefix, uint8_t nTrim, KV_Key buffer, uint32_t offset, uint32_t* nKeys){
    size_t remaining = KV_LIST_BUFFER_SIZE;
    uint32_t nRequiredKeys = *nKeys>0?*nKeys:UINT32_MAX;
    uint32_t nMatchingKeys = 0;

    klc_scanner scanner;
    struct klc_key prefix_key;
    struct klc_key kreon_key;

    prefix_key.size = strlen(prefix);
    prefix_key.data = prefix;
    scanner = klc_init_scanner(handle, &prefix_key, KLC_GREATER_OR_EQUAL);
    if (!scanner)
        return KV_FAILURE;
    while (klc_is_valid(scanner)) {
        kreon_key = klc_get_key(scanner);
        if (memcmp(prefix, kreon_key.data, strlen(prefix)) != 0)
            break;

        LogActivity(H3_DEBUG_MSG, "key size: %u data: '%*s'\n", kreon_key.size, kreon_key.size, kreon_key.data);

        if(offset)
            offset--;

        else {

            // Copy the keys if a buffer is provided...
            if(buffer){
                size_t entrySize = kreon_key.size - nTrim;
                if(remaining >= entrySize){
                    memcpy(&buffer[KV_LIST_BUFFER_SIZE - remaining], &kreon_key.data[nTrim], entrySize);
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
        if (nMatchingKeys >= nRequiredKeys)
            break;

        if (klc_get_next(scanner) && !klc_is_valid(scanner))
            break;
    }
    klc_close_scanner(scanner);

    *nKeys = nMatchingKeys;
    return KV_SUCCESS;
}

KV_Status KV_Kreon_Exists(KV_Handle handle, KV_Key key) {
    struct klc_key kreon_key;
    kreon_key.size = strlen(key) + 1;
    kreon_key.data = key;
    if (klc_exists(handle, &kreon_key) == KLC_SUCCESS)
        return KV_KEY_EXIST;

    return KV_KEY_NOT_EXIST;
}

KV_Status KV_Kreon_Read(KV_Handle handle, KV_Key key, off_t offset, KV_Value* value, size_t* size) {
    KV_Status status = KV_FAILURE;

    size_t segmentSize;
    char *segment = NULL;

    struct klc_key kreon_key;
    kreon_key.size = strlen(key)+1;
    kreon_key.data = key;
    struct klc_value kreon_value;
    struct klc_value *kreon_value_p;
    kreon_value_p = &kreon_value;
    switch (klc_get(handle, &kreon_key, &kreon_value_p)) {
        case KLC_SUCCESS:
            if (offset > kreon_value.size){
                *size = 0;
                return KV_SUCCESS;
            }

            if (*value == NULL) {
                segmentSize = kreon_value.size - offset;
                if ((segment = malloc(segmentSize))) {
                    memcpy(segment, kreon_value.data + offset, segmentSize);
                    *value = (KV_Value)segment;
                    *size = segmentSize;
                    status = KV_SUCCESS;
                }
            } else {
                segmentSize = min(kreon_value.size - offset, *size);
                memcpy(*value, kreon_value.data + offset, segmentSize);
                *size = segmentSize;
                status = KV_SUCCESS;
            }

            break;
        case KLC_KEY_NOT_FOUND:
            return KV_KEY_NOT_EXIST;
        default:
            return KV_FAILURE;
    }

    return status;
}

KV_Status KV_Kreon_Update(KV_Handle handle, KV_Key key, KV_Value value, off_t offset, size_t size) {
    KV_Status status;

    KV_Value newValue;
    size_t newSize;

    struct klc_key kreon_key;
    kreon_key.size = strlen(key)+1;
    kreon_key.data = key;
    struct klc_value kreon_value;
    struct klc_value *kreon_value_p;
    kreon_value_p = &kreon_value;
    switch (klc_get(handle, &kreon_key, &kreon_value_p)) {
        case KLC_SUCCESS:
            if (offset + size <= kreon_value.size) {
                newSize = kreon_value.size;
            } else {
                newSize = offset + size;
            }
            newValue = (KV_Value)malloc(newSize);
            memcpy(newValue, kreon_value.data, kreon_value.size);
            memcpy(newValue + offset, value, size);
            break;
        case KLC_KEY_NOT_FOUND:
            newSize = offset + size;
            newValue = (KV_Value)calloc(1, newSize);
            memcpy(newValue + offset, value, size);
            break;
        default:
            return KV_FAILURE;
    }

    // Convert key blob to string
    struct klc_key_value kreon_key_value;
    kreon_key_value.k.size = strlen(key)+1;
    kreon_key_value.k.data = key;
    kreon_key_value.v.size = newSize;
    kreon_key_value.v.data = (char *)newValue;
    if(klc_put(handle, &kreon_key_value) == KLC_SUCCESS) {
        status = KV_SUCCESS;
    } else {
        status = KV_FAILURE;
    }
    free(newValue);

    return status;
}

KV_Status KV_Kreon_Write(KV_Handle handle, KV_Key key, KV_Value value, size_t size) {

    // Convert key blob to string
    struct klc_key_value kreon_key_value;
    kreon_key_value.k.size = strlen(key)+1;
    kreon_key_value.k.data = key;
    kreon_key_value.v.size = size;
    kreon_key_value.v.data = (char *)value;
    if(klc_put(handle, &kreon_key_value) == KLC_SUCCESS)
        return KV_SUCCESS;

    return KV_FAILURE;
}

KV_Status KV_Kreon_Delete(KV_Handle handle, KV_Key key) {

    KV_Status status;

    struct klc_key kreon_key;
    kreon_key.size = strlen(key) + 1;
    kreon_key.data = key;
    switch(klc_delete(handle, &kreon_key)){
        case KLC_SUCCESS: status = KV_SUCCESS;              break;
        case KLC_KEY_NOT_FOUND: status = KV_KEY_NOT_EXIST;  break;
        default:                status = KV_FAILURE;        break;

    }

    return status;
}

KV_Status KV_Kreon_Create(KV_Handle handle, KV_Key key, KV_Value value, size_t size){
    KV_Status status;

    if( (status = KV_Kreon_Exists(handle, key)) == KV_KEY_NOT_EXIST){
         status = KV_Kreon_Write(handle, key, value, size);
    }

    return status;
}

KV_Status KV_Kreon_Copy(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
    KV_Status status;

    if((status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS){
        status = KV_Kreon_Write(handle, dest_key, value, size);
    }

    return status;
}

KV_Status KV_Kreon_Move(KV_Handle handle, KV_Key src_key, KV_Key dest_key) {
    size_t size = 0x00;
    KV_Value value = NULL;
    KV_Status status;

    if( (status = KV_Kreon_Read(handle, src_key, 0, &value, &size)) == KV_SUCCESS &&
        (status = KV_Kreon_Write(handle, dest_key, value, size)) == KV_SUCCESS      ){
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
    .storage_info = NULL,
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

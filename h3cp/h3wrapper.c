// Copyright [2020] [FORTH-ICS]
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

#include <sys/stat.h>
#include <fcntl.h>

#include "h3wrapper.h"

#define TRUE 1;
#define FALSE 0;

int h3lib_write_object_from_file(H3_Handle handle, char *bucketName, char *objectName, char *filename) {
    off_t offset = 0;
    uint32_t userId = 0;

    H3_Auth auth;
    auth.userId = userId;

    struct stat st;
    if (stat(filename, &st) == -1) {
        return FALSE;
    }
    size_t size = st.st_size;
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        return FALSE;
    }

    H3_Status return_value = H3_WriteObjectFromFile(handle, &auth, bucketName, objectName, fd, size, offset);
    if (return_value != H3_SUCCESS && return_value != H3_CONTINUE) {
        close(fd);
        return FALSE;
    }
    close(fd);

    return TRUE;
}

int h3lib_read_object_to_file(H3_Handle handle, char *bucketName, char *objectName, char *filename) {
    off_t offset = 0;
    size_t size = 0;
    uint32_t userId = 0;

    H3_Auth auth;
    auth.userId = userId;

    int fd = open(filename, O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (fd == -1) {
        return FALSE;
    }

    H3_Status return_value = H3_ReadObjectToFile(handle, &auth, bucketName, objectName, offset, fd, &size);
    if (return_value != H3_SUCCESS && return_value != H3_CONTINUE) {
        close(fd);
        return FALSE;
    }
    close(fd);

    return TRUE;
}

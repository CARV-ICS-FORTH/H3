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

#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <h3lib/h3lib.h>

#include <sys/stat.h>
#include <fcntl.h>

#define TIMESPEC_TO_DOUBLE(t) (t.tv_sec + ((double)t.tv_nsec / 1000000000ULL))

// Named tuples returned
static PyTypeObject bucket_stats_type;
static PyTypeObject bucket_info_type;
static PyTypeObject object_info_type;
static PyTypeObject part_info_type;

static PyStructSequence_Field bucket_stats_fields[] = {
    {"size",              NULL},
    {"count",             NULL},
    {"last_access",       NULL},
    {"last_modification", NULL},
    {NULL}
};

static PyStructSequence_Field bucket_info_fields[] = {
    {"creation", NULL},
    {"stats",    NULL},
    {NULL}
};

static PyStructSequence_Field object_info_fields[] = {
    {"is_bad",            NULL},
    {"size",              NULL},
    {"creation",          NULL},
    {"last_access",       NULL},
    {"last_modification", NULL},
    {"last_change",       NULL},
    {"mode",              NULL},
    {"uid",               NULL},
    {"gid",               NULL},
    {NULL}
};

static PyStructSequence_Field part_info_fields[] = {
    {"part_number", NULL},
    {"size",        NULL},
    {NULL}
};

static PyStructSequence_Desc bucket_stats_desc = {
    "pyh3lib.h3lib.bucket_stats",
    NULL,
    bucket_stats_fields,
    4,
};

static PyStructSequence_Desc bucket_info_desc = {
    "pyh3lib.h3lib.bucket_info",
    NULL,
    bucket_info_fields,
    2,
};

static PyStructSequence_Desc object_info_desc = {
    "pyh3lib.h3lib.object_info",
    NULL,
    object_info_fields,
    5,
};

static PyStructSequence_Desc part_info_desc = {
    "pyh3lib.h3lib.part_info",
    NULL,
    part_info_fields,
    2,
};

// Exceptions raised
static PyObject *failure_status;
static PyObject *invalid_args_status;
static PyObject *store_error_status;
static PyObject *exists_status;
static PyObject *not_exists_status;
static PyObject *name_too_long_status;
static PyObject *not_empty_status;

static int did_raise_exception(H3_Status status) {
    switch (status) {
        case H3_FAILURE:
            PyErr_SetNone(failure_status);
            return 1;
        case H3_INVALID_ARGS:
            PyErr_SetNone(invalid_args_status);
            return 1;
        case H3_STORE_ERROR:
            PyErr_SetNone(store_error_status);
            return 1;
        case H3_EXISTS:
            PyErr_SetNone(exists_status);
            return 1;
        case H3_NOT_EXISTS:
            PyErr_SetNone(not_exists_status);
            return 1;
        case H3_NAME_TOO_LONG:
            PyErr_SetNone(name_too_long_status);
            return 1;
        case H3_NOT_EMPTY:
            PyErr_SetNone(not_empty_status);
            return 1;
        case H3_SUCCESS:
            return 0;
        case H3_CONTINUE:
            return 0;
    }
}

static PyObject *h3lib_version(PyObject *self) {
    return Py_BuildValue("s", H3_Version());
}

// Library calls
void h3lib_free(PyObject *capsule) {
    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return;

    H3_Free(handle);
}

static PyObject *h3lib_init(PyObject* self, PyObject *args, PyObject *kw) {
    char *storageUri;

    static char *kwlist[] = {"storage_uri", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &storageUri))
        return NULL;

    H3_Handle handle = H3_Init(storageUri);
    if (handle == NULL) {
        PyErr_SetNone(invalid_args_status);
        return NULL;
    }

    return PyCapsule_New((void *)handle, NULL, h3lib_free);
}

static PyObject *h3lib_list_buckets(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "O|I", kwlist, &capsule, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_Name bucketNameArray = NULL;
    uint32_t nBuckets;

    auth.userId = userId;
    if (did_raise_exception(H3_ListBuckets(handle, &auth, &bucketNameArray, &nBuckets)))
        return NULL;

    PyObject *list = PyList_New(nBuckets);
    uint32_t i;
    H3_Name current_name;
    uint32_t current_name_pos = 0;
    size_t current_name_len = 0;
    for (i = 0; i < nBuckets; i ++) {
        current_name = &(bucketNameArray[current_name_pos]);
        current_name_len = strlen(current_name);
        current_name_pos += current_name_len;
        while (bucketNameArray[current_name_pos] == '\0')
            current_name_pos++;

        PyList_SET_ITEM(list, i, Py_BuildValue("s", current_name));
    }
    free(bucketNameArray);

    return list;
}

static PyObject *h3lib_info_bucket(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    uint8_t getStats = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "get_stats", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|bI", kwlist, &capsule, &bucketName, &getStats, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_BucketInfo bucketInfo;
    auth.userId = userId;
    if (did_raise_exception(H3_InfoBucket(handle, &auth, bucketName, &bucketInfo, getStats)))
        return NULL;

    PyObject *bucket_stats;
    if (getStats) {
        bucket_stats = PyStructSequence_New(&bucket_stats_type);
        if (bucket_stats == NULL) {
            PyErr_NoMemory();
            return NULL;
        }
        PyStructSequence_SET_ITEM(bucket_stats, 0, Py_BuildValue("k", bucketInfo.stats.size));
        PyStructSequence_SET_ITEM(bucket_stats, 1, Py_BuildValue("K", bucketInfo.stats.nObjects));
        PyStructSequence_SET_ITEM(bucket_stats, 2, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(bucketInfo.stats.lastAccess)));
        PyStructSequence_SET_ITEM(bucket_stats, 3, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(bucketInfo.stats.lastModification)));
        if (PyErr_Occurred()) {
            Py_DECREF(bucket_stats);
            PyErr_NoMemory();
            return NULL;
        }
    }

    PyObject *bucket_info = PyStructSequence_New(&bucket_info_type);
    if (bucket_info == NULL) {
        if (getStats)
            Py_DECREF(bucket_stats);
        PyErr_NoMemory();
        return NULL;
    }
    PyStructSequence_SET_ITEM(bucket_info, 0, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(bucketInfo.creation)));
    if (getStats) {
        PyStructSequence_SET_ITEM(bucket_info, 1, bucket_stats);
        // XXX Py_DECREF(bucket_stats);...
    }
    if (PyErr_Occurred()) {
        Py_DECREF(bucket_info);
        if (getStats)
            Py_DECREF(bucket_stats);
        PyErr_NoMemory();
        return NULL;
    }

    return bucket_info;
}

static PyObject *h3lib_create_bucket(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &bucketName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CreateBucket(handle, &auth, bucketName)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_delete_bucket(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &bucketName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_DeleteBucket(handle, &auth, bucketName)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_purge_bucket(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &bucketName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_PurgeBucket(handle, &auth, bucketName)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_list_objects(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    char *prefix = "";
    uint32_t offset = 0;
    uint32_t count = 10000;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "prefix", "offset", "count", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|skkI", kwlist, &capsule, &bucketName, &prefix, &offset, &count, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_Name objectNameArray = NULL;
    uint32_t nObjects = count;

    auth.userId = userId;
    H3_Status return_value = H3_ListObjects(handle, &auth, bucketName, prefix, offset, &objectNameArray, &nObjects);
    if (did_raise_exception(return_value))
        return NULL;

    PyObject *list = PyList_New(nObjects);
    uint32_t i;
    H3_Name current_name;
    uint32_t current_name_pos = 0;
    size_t current_name_len = 0;
    for (i = 0; i < nObjects; i ++) {
        current_name = &(objectNameArray[current_name_pos]);
        current_name_len = strlen(current_name);
        current_name_pos += current_name_len;
        while (objectNameArray[current_name_pos] == '\0')
            current_name_pos++;

        PyList_SET_ITEM(list, i, Py_BuildValue("s", current_name));
    }
    free(objectNameArray);

    return Py_BuildValue("(OO)", list, (return_value == H3_SUCCESS ? Py_True : Py_False));
}

static PyObject *h3lib_info_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Oss|I", kwlist, &capsule, &bucketName, &objectName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_ObjectInfo objectInfo;

    auth.userId = userId;
    if (did_raise_exception(H3_InfoObject(handle, &auth, bucketName, objectName, &objectInfo)))
        return NULL;

    PyObject *object_info = PyStructSequence_New(&object_info_type);
    if (object_info == NULL) {
        PyErr_NoMemory();
        return NULL;
    }
    PyStructSequence_SET_ITEM(object_info, 0, Py_BuildValue("O", (objectInfo.isBad ? Py_True : Py_False)));
    PyStructSequence_SET_ITEM(object_info, 1, Py_BuildValue("k", objectInfo.size));
    PyStructSequence_SET_ITEM(object_info, 2, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(objectInfo.creation)));
    PyStructSequence_SET_ITEM(object_info, 3, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(objectInfo.lastAccess)));
    PyStructSequence_SET_ITEM(object_info, 4, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(objectInfo.lastModification)));
    PyStructSequence_SET_ITEM(object_info, 5, Py_BuildValue("d", TIMESPEC_TO_DOUBLE(objectInfo.lastChange)));
    PyStructSequence_SET_ITEM(object_info, 6, Py_BuildValue("i", objectInfo.mode));
    PyStructSequence_SET_ITEM(object_info, 7, Py_BuildValue("i", objectInfo.uid));
    PyStructSequence_SET_ITEM(object_info, 8, Py_BuildValue("i", objectInfo.gid));
    if (PyErr_Occurred()) {
        Py_DECREF(object_info);
        PyErr_NoMemory();
        return NULL;
    }

    return object_info;
}

static PyObject *h3lib_set_object_permissions(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    int mode;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "mode", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Ossii|I", kwlist, &capsule, &bucketName, &objectName, &mode, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_Attribute attribute;

    auth.userId = userId;
    attribute.type = H3_ATTRIBUTE_PERMISSIONS;
    attribute.mode = mode;
    if (did_raise_exception(H3_SetObjectAttributes(handle, &auth, bucketName, objectName, attribute)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_set_object_owner(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    int uid;
    int gid;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "uid", "gid", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Ossii|I", kwlist, &capsule, &bucketName, &objectName, &uid, &gid, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_Attribute attribute;

    auth.userId = userId;
    attribute.type = H3_ATTRIBUTE_OWNER;
    attribute.uid = uid;
    attribute.gid = gid;
    if (did_raise_exception(H3_SetObjectAttributes(handle, &auth, bucketName, objectName, attribute)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_create_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    const char *data;
    size_t size;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "data", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Ossy#|I", kwlist, &capsule, &bucketName, &objectName, &data, &size, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CreateObject(handle, &auth, bucketName, objectName, (void *)data, size)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_create_object_copy(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name srcObjectName;
    off_t offset;
    size_t size;
    H3_Name dstObjectName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "src_object_name", "offset", "size", "dst_object_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osslks|I", kwlist, &capsule, &bucketName, &srcObjectName, &offset, &size, &dstObjectName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CreateObjectCopy(handle, &auth, bucketName, srcObjectName, offset, &size, dstObjectName)))
        return NULL;

    return Py_BuildValue("k", size);
}

static PyObject *h3lib_create_object_from_file(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    const char *filename;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "filename", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|I", kwlist, &capsule, &bucketName, &objectName, &filename, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;

    struct stat st;
    if (stat(filename, &st) == -1) {
        PyErr_SetNone(failure_status);
        return NULL;
    }
    size_t size = st.st_size;
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        PyErr_SetNone(failure_status);
        return NULL;
    }

    if (did_raise_exception(H3_CreateObjectFromFile(handle, &auth, bucketName, objectName, fd, size))) {
        close(fd);
        return NULL;
    }
    close(fd);

    Py_RETURN_TRUE;
}

static PyObject *h3lib_write_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    const char *data;
    size_t size;
    off_t offset = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "data", "offset", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Ossy#|lI", kwlist, &capsule, &bucketName, &objectName, &data, &size, &offset, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_WriteObject(handle, &auth, bucketName, objectName, (void *)data, size, offset)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_write_object_copy(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name srcObjectName;
    off_t srcOffset;
    size_t size;
    H3_Name dstObjectName;
    off_t dstOffset;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "src_object_name", "src_offset", "size", "dst_object_name", "dst_offset", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osslksl|I", kwlist, &capsule, &bucketName, &srcObjectName, &srcOffset, &size, &dstObjectName, &dstOffset, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_WriteObjectCopy(handle, &auth, bucketName, srcObjectName, srcOffset, &size, dstObjectName, dstOffset)))
        return NULL;

    return Py_BuildValue("k", size);
}

static PyObject *h3lib_write_object_from_file(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    const char *filename;
    off_t offset = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "filename", "offset", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|lI", kwlist, &capsule, &bucketName, &objectName, &filename, &offset, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;

    struct stat st;
    if (stat(filename, &st) == -1) {
        PyErr_SetNone(failure_status);
        return NULL;
    }
    size_t size = st.st_size;
    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        PyErr_SetNone(failure_status);
        return NULL;
    }

    if (did_raise_exception(H3_WriteObjectFromFile(handle, &auth, bucketName, objectName, fd, size, offset))) {
        close(fd);
        return NULL;
    }
    close(fd);

    Py_RETURN_TRUE;
}

static PyObject *h3lib_read_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    off_t offset = 0;
    size_t size = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "offset", "size", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Oss|lkI", kwlist, &capsule, &bucketName, &objectName, &offset, &size, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    void *data = NULL;

    // h3lib will only allocate a buffer if size = 0 AND data = NULL.
    // In all other cases it expects an appropriately sized buffer to
    // be allocated by the caller.
    if (size) {
        data = malloc(size);
        if (!data)
            return PyErr_NoMemory();
    }

    auth.userId = userId;
    H3_Status return_value = H3_ReadObject(handle, &auth, bucketName, objectName, offset, &data, &size);
    if (did_raise_exception(return_value))
        return NULL;

    PyObject *data_object = Py_BuildValue("y#", data, size);
    free(data);

    return Py_BuildValue("(OO)", data_object, (return_value == H3_SUCCESS ? Py_True : Py_False));
}

static PyObject *h3lib_read_object_to_file(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    const char *filename;
    off_t offset = 0;
    size_t size = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "filename", "offset", "size", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|lkI", kwlist, &capsule, &bucketName, &objectName, &filename, &offset, &size, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;

    int fd = open(filename, O_CREAT|O_WRONLY|O_TRUNC, 0644);
    if (fd == -1) {
        PyErr_SetNone(failure_status);
        return NULL;
    }

    H3_Status return_value = H3_ReadObjectToFile(handle, &auth, bucketName, objectName, offset, fd, &size);
    if (did_raise_exception(return_value)) {
        close(fd);
        return NULL;
    }
    close(fd);

    return Py_BuildValue("(OO)", Py_None, (return_value == H3_SUCCESS ? Py_True : Py_False));
}

static PyObject *h3lib_copy_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name srcObjectName;
    H3_Name dstObjectName;
    uint8_t noOverwrite = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "src_object_name", "dst_object_name", "no_overwrite", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|bI", kwlist, &capsule, &bucketName, &srcObjectName, &dstObjectName, &noOverwrite, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CopyObject(handle, &auth, bucketName, srcObjectName, dstObjectName, noOverwrite)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_move_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name srcObjectName;
    H3_Name dstObjectName;
    uint8_t noOverwrite = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "src_object_name", "dst_object_name", "no_overwrite", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|bI", kwlist, &capsule, &bucketName, &srcObjectName, &dstObjectName, &noOverwrite, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_MoveObject(handle, &auth, bucketName, srcObjectName, dstObjectName, noOverwrite)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_exchange_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name srcObjectName;
    H3_Name dstObjectName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "src_object_name", "dst_object_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Osss|I", kwlist, &capsule, &bucketName, &srcObjectName, &dstObjectName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_ExchangeObject(handle, &auth, bucketName, srcObjectName, dstObjectName)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_truncate_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    size_t size = 0;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "size", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Oss|kI", kwlist, &capsule, &bucketName, &objectName, &size, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_TruncateObject(handle, &auth, bucketName, objectName, size)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_delete_object(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Oss|I", kwlist, &capsule, &bucketName, &objectName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_DeleteObject(handle, &auth, bucketName, objectName)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_list_multiparts(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    uint32_t offset = 0;
    uint32_t count = 10000;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "offset", "count", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|kkI", kwlist, &capsule, &bucketName, &offset, &count, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_MultipartId multipartIdArray = NULL;
    uint32_t nIds = count;

    auth.userId = userId;
    H3_Status return_value;
    return_value = H3_ListMultiparts(handle, &auth, bucketName, offset, &multipartIdArray, &nIds);
    if (did_raise_exception(return_value))
        return NULL;

    PyObject *list = PyList_New(nIds);
    uint32_t i;
    H3_Name current_name;
    uint32_t current_name_pos = 0;
    size_t current_name_len = 0;
    for (i = 0; i < nIds; i ++) {
        current_name = &(multipartIdArray[current_name_pos]);
        current_name_len = strlen(current_name);
        current_name_pos += current_name_len;
        while (multipartIdArray[current_name_pos] == '\0')
            current_name_pos++;

        PyList_SET_ITEM(list, i, Py_BuildValue("s", current_name));
    }
    free(multipartIdArray);

    return Py_BuildValue("(OO)", list, (return_value == H3_SUCCESS ? Py_True : Py_False));
}

static PyObject *h3lib_create_multipart(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name bucketName;
    H3_Name objectName;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "bucket_name", "object_name", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Oss|I", kwlist, &capsule, &bucketName, &objectName, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_MultipartId multipartId;

    auth.userId = userId;
    if (did_raise_exception(H3_CreateMultipart(handle, &auth, bucketName, objectName, &multipartId)))
        return NULL;

    return Py_BuildValue("s", multipartId);
}

static PyObject *h3lib_complete_multipart(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_MultipartId multipartId;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "multipart_id", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &multipartId, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CompleteMultipart(handle, &auth, multipartId)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_abort_multipart(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_MultipartId multipartId;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "multipart_id", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &multipartId, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_AbortMultipart(handle, &auth, multipartId)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_list_parts(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_MultipartId multipartId;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "multipart_id", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "Os|I", kwlist, &capsule, &multipartId, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;
    H3_PartInfo *partInfoArray;
    uint32_t nParts;

    auth.userId = userId;
    if (did_raise_exception(H3_ListParts(handle, &auth, multipartId, &partInfoArray, &nParts)))
        return NULL;

    PyObject *list = PyList_New(nParts);
    size_t i;
    H3_PartInfo *partInfo;
    PyObject *part_info;
    for (i = 0; i < nParts; i ++) {
        partInfo = &(partInfoArray[i]);

        part_info = PyStructSequence_New(&part_info_type);
        if (part_info == NULL) {
            // XXX Decrease list reference...
            PyErr_NoMemory();
            return NULL;
        }
        PyStructSequence_SET_ITEM(part_info, 0, Py_BuildValue("I", partInfo->partNumber));
        PyStructSequence_SET_ITEM(part_info, 1, Py_BuildValue("k", partInfo->size));
        if (PyErr_Occurred()) {
            Py_DECREF(part_info);
            // XXX Decrease list reference...
            PyErr_NoMemory();
            return NULL;
        }

        PyList_SET_ITEM(list, i, part_info);
        // XXX Py_DECREF(part_info);...
    }

    return list;
}

static PyObject *h3lib_create_part(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_MultipartId multipartId;
    uint32_t partNumber;
    const char *data;
    size_t size;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "multipart_id", "part_number", "data", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OsIy#|I", kwlist, &capsule, &multipartId, &partNumber, &data, &size, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CreatePart(handle, &auth, multipartId, partNumber, (void *)data, size)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyObject *h3lib_create_part_copy(PyObject* self, PyObject *args, PyObject *kw) {
    PyObject *capsule = NULL;
    H3_Name objectName;
    off_t offset;
    size_t size;
    H3_MultipartId multipartId;
    uint32_t partNumber;
    uint32_t userId = 0;

    static char *kwlist[] = {"handle", "object_name", "offset", "size", "multipart_id", "part_number", "user_id", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kw, "OslksI|I", kwlist, &capsule, &objectName, &offset, &size, &multipartId, &partNumber, &userId))
        return NULL;

    H3_Handle handle = (H3_Handle)PyCapsule_GetPointer(capsule, NULL);
    if (handle == NULL)
        return NULL;

    H3_Auth auth;

    auth.userId = userId;
    if (did_raise_exception(H3_CreatePartCopy(handle, &auth, objectName, offset, size, multipartId, partNumber)))
        return NULL;

    Py_RETURN_TRUE;
}

static PyMethodDef module_functions[] = {
    {"version",                 (PyCFunction)h3lib_version,                 METH_NOARGS, NULL},
    {"init",                    (PyCFunction)h3lib_init,                    METH_VARARGS|METH_KEYWORDS, NULL},

    {"list_buckets",            (PyCFunction)h3lib_list_buckets,            METH_VARARGS|METH_KEYWORDS, NULL},
    {"info_bucket",             (PyCFunction)h3lib_info_bucket,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_bucket",           (PyCFunction)h3lib_create_bucket,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"delete_bucket",           (PyCFunction)h3lib_delete_bucket,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"purge_bucket",            (PyCFunction)h3lib_purge_bucket,            METH_VARARGS|METH_KEYWORDS, NULL},

    {"list_objects",            (PyCFunction)h3lib_list_objects,            METH_VARARGS|METH_KEYWORDS, NULL},
    {"info_object",             (PyCFunction)h3lib_info_object,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"set_object_permissions",  (PyCFunction)h3lib_set_object_permissions,  METH_VARARGS|METH_KEYWORDS, NULL},
    {"set_object_owner",        (PyCFunction)h3lib_set_object_owner,        METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_object",           (PyCFunction)h3lib_create_object,           METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_object_copy",      (PyCFunction)h3lib_create_object_copy,      METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_object_from_file", (PyCFunction)h3lib_create_object_from_file, METH_VARARGS|METH_KEYWORDS, NULL},
    {"write_object",            (PyCFunction)h3lib_write_object,            METH_VARARGS|METH_KEYWORDS, NULL},
    {"write_object_copy",       (PyCFunction)h3lib_write_object_copy,       METH_VARARGS|METH_KEYWORDS, NULL},
    {"write_object_from_file",  (PyCFunction)h3lib_write_object_from_file,  METH_VARARGS|METH_KEYWORDS, NULL},
    {"read_object",             (PyCFunction)h3lib_read_object,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"read_object_to_file",     (PyCFunction)h3lib_read_object_to_file,     METH_VARARGS|METH_KEYWORDS, NULL},
    {"copy_object",             (PyCFunction)h3lib_copy_object,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"move_object",             (PyCFunction)h3lib_move_object,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"exchange_object",         (PyCFunction)h3lib_exchange_object,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"truncate_object",         (PyCFunction)h3lib_truncate_object,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"delete_object",           (PyCFunction)h3lib_delete_object,           METH_VARARGS|METH_KEYWORDS, NULL},

    {"list_multiparts",         (PyCFunction)h3lib_list_multiparts,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_multipart",        (PyCFunction)h3lib_create_multipart,        METH_VARARGS|METH_KEYWORDS, NULL},
    {"complete_multipart",      (PyCFunction)h3lib_complete_multipart,      METH_VARARGS|METH_KEYWORDS, NULL},
    {"abort_multipart",         (PyCFunction)h3lib_abort_multipart,         METH_VARARGS|METH_KEYWORDS, NULL},
    {"list_parts",              (PyCFunction)h3lib_list_parts,              METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_part",             (PyCFunction)h3lib_create_part,             METH_VARARGS|METH_KEYWORDS, NULL},
    {"create_part_copy",        (PyCFunction)h3lib_create_part_copy,        METH_VARARGS|METH_KEYWORDS, NULL},

    {NULL}
};

static struct PyModuleDef module_definition = {
    PyModuleDef_HEAD_INIT,
    "h3lib",
    "Python interface to H3",
    -1,
    module_functions
};

PyMODINIT_FUNC PyInit_h3lib(void) {
    PyObject *module = PyModule_Create(&module_definition);

    PyModule_AddIntConstant(module, "H3_BUCKET_NAME_SIZE", H3_BUCKET_NAME_SIZE);
    PyModule_AddIntConstant(module, "H3_OBJECT_NAME_SIZE", H3_OBJECT_NAME_SIZE);

    PyStructSequence_InitType(&bucket_stats_type, &bucket_stats_desc);
    PyStructSequence_InitType(&bucket_info_type, &bucket_info_desc);
    PyStructSequence_InitType(&object_info_type, &object_info_desc);
    PyStructSequence_InitType(&part_info_type, &part_info_desc);
    Py_INCREF((PyObject *)&bucket_stats_type);
    Py_INCREF((PyObject *)&bucket_info_type);
    Py_INCREF((PyObject *)&object_info_type);
    Py_INCREF((PyObject *)&part_info_type);
    // PyModule_AddObject(module, "bucket_stats", (PyObject *)&bucket_stats_type);
    // PyModule_AddObject(module, "bucket_info", (PyObject *)&bucket_info_type);
    // PyModule_AddObject(module, "object_info", (PyObject *)&object_info_type);
    // PyModule_AddObject(module, "part_info", (PyObject *)&part_info_type);

    failure_status = PyErr_NewException("pyh3lib.h3lib.FailureError", NULL, NULL);
    invalid_args_status = PyErr_NewException("pyh3lib.h3lib.InvalidArgsError", NULL, NULL);
    store_error_status = PyErr_NewException("pyh3lib.h3lib.StoreError", NULL, NULL);
    exists_status = PyErr_NewException("pyh3lib.h3lib.ExistsError", NULL, NULL);
    not_exists_status = PyErr_NewException("pyh3lib.h3lib.NotExistsError", NULL, NULL);
    name_too_long_status = PyErr_NewException("pyh3lib.h3lib.NameTooLongError", NULL, NULL);
    not_empty_status = PyErr_NewException("pyh3lib.h3lib.NotEmptyError", NULL, NULL);
    PyModule_AddObject(module, "FailureError", failure_status);
    PyModule_AddObject(module, "InvalidArgsError", invalid_args_status);
    PyModule_AddObject(module, "StoreError", store_error_status);
    PyModule_AddObject(module, "ExistsError", exists_status);
    PyModule_AddObject(module, "NotExistsError", not_exists_status);
    PyModule_AddObject(module, "NameTooLongError", name_too_long_status);
    PyModule_AddObject(module, "NotEmptyError", not_empty_status);

    return module;
}

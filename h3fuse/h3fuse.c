
#define FUSE_USE_VERSION 39

#define _GNU_SOURCE

#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <glib.h>
#include <fuse3/fuse.h>
#include <h3lib/h3lib.h>

#include "h3fuse_config.h"

#define H3_FUSE_MAX_FILENAME	255	// This is restricted by KV_FS plugin in h3lib

#ifndef RENAME_NOREPLACE
#define RENAME_NOREPLACE	(1 << 0)	/* Don't overwrite target */
#endif

#ifndef RENAME_EXCHANGE
#define RENAME_EXCHANGE		(1 << 1)	/* Exchange source and dest */
#endif

#ifndef RENAME_WHITEOUT
#define RENAME_WHITEOUT		(1 << 2)	/* Whiteout source */
#endif

enum {
     KEY_HELP,
     KEY_VERSION,
};

typedef struct {
     char* storageUri;
     char* token;
     char* bucket;
} H3FS_Config;

typedef struct {
    H3_Handle handle;
    H3_Auth token;
    H3_Name bucket;
}H3FS_PrivateData;

extern FILE* stderr;

static H3FS_PrivateData data;

static inline H3_Name Cast2DirEntry(H3_Name dirEntry, int* isDir){
	char* slash;

	// Ignore leading slash
	if(dirEntry[0] == '/'){
		dirEntry++;
	}

	// Drop the directory content since we are not interested in it.
	if((slash = strchr(dirEntry, '/'))){
		*slash = '\0';
		*isDir = 1;
	}
	else
		*isDir = 0;

//	printf("%s: %s %s\n", __FUNCTION__, dirEntry, *isDir?"Dir":"File");

	return dirEntry;
}

static int GetObjectInfo(const char* path, struct stat* stbuf){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    H3_ObjectInfo info;

    switch(H3_InfoObject(data.handle, &data.token, data.bucket, object, &info)){
		case H3_SUCCESS:
			stbuf->st_mode = S_IFREG | info.mode;
			stbuf->st_nlink = 1;
			stbuf->st_size = info.size;
			stbuf->st_atim = info.lastAccess;
			stbuf->st_mtim = info.lastModification;
			stbuf->st_ctim = info.lastChange;
			stbuf->st_uid = info.uid;
			stbuf->st_gid = info.gid;
			break;

		case H3_INVALID_ARGS: 	res = -EINVAL; break;
		case H3_NAME_TOO_LONG: 	res = -ENAMETOOLONG; break;
		case H3_NOT_EXISTS: 	res = -ENOENT; break;
		default: 				res = -EIO; break;
    }


    // We have to consider the case the path is a sub-directory.
    if(res == -ENOENT){
    	H3_Name objectNameArray;
    	H3_Name directory = NULL;
    	uint32_t nObjects = 0;

    	// Note FUSE always strip the trailing '/' from directories
    	asprintf(&directory, "%s/", object);

    	// Either a fake one, i.e. mkdir lala, or
    	if( H3_InfoObject(data.handle, &data.token, data.bucket, directory, &info) == H3_SUCCESS){
    		stbuf->st_mode = S_IFDIR | info.mode;
    		stbuf->st_nlink = 2;
    		stbuf->st_atim = info.lastAccess;
    		stbuf->st_mtim = info.lastModification;
    		stbuf->st_ctim = info.lastChange;
    		stbuf->st_uid = info.uid;
    		stbuf->st_gid = info.gid;
    		res = 0;
    	}

    	// ...a real one, i.e. listing an externally populated bucket
    	else if( H3_ListObjects(data.handle, &data.token, data.bucket, directory, 0, &objectNameArray, &nObjects) == H3_SUCCESS){
    		if(nObjects){
    			stbuf->st_mode = S_IFDIR | 0755;
    			stbuf->st_nlink = 2;
    			res = 0;
    		}
    	}

    	free(directory);
    }

    return res;
}

static int H3FS_GetAttr(const char* path, struct stat* stbuf, struct fuse_file_info* fi){

    if (strcmp(path, "/") == 0 ) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    return GetObjectInfo(path, stbuf);
}

static int H3FS_MkNod(const char* path, mode_t mode, dev_t dev){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

	(void)dev;

	if(!length || (!S_ISDIR(mode) && !S_ISREG(mode)) ){
		return -EINVAL;
	}

	switch(H3_CreateObject(data.handle, &data.token, data.bucket, object, NULL, 0)){
		case H3_FAILURE:
		case H3_INVALID_ARGS: 	res = -EINVAL; break;
		case H3_EXISTS: 		res = -EEXIST; break;
		case H3_NAME_TOO_LONG: 	res = -ENAMETOOLONG; break;
		default: break;
	}

	H3_Attribute attrib = {.type = H3_ATTRIBUTE_PERMISSIONS, .mode = mode};
	if(res == 0 && H3_SetObjectAttributes(data.handle, &data.token, data.bucket, object, attrib) != H3_SUCCESS){
		res = -EINVAL;
	}

	return res;
}


static int H3FS_MkDir(const char* path, mode_t mode){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length){
    	H3_Status status;
        H3_Name objectNameArray;
        H3_Name directory = NULL;
        uint32_t nObjects = 1;

        asprintf(&directory, "%s/", object);
    	if((status = H3_ListObjects(data.handle, &data.token, data.bucket, directory, 0, &objectNameArray, &nObjects))  == H3_SUCCESS || status == H3_CONTINUE  ){
    		if(!nObjects){
    			if( (status = H3_CreateObject(data.handle, &data.token, data.bucket, directory, NULL, 0)) == H3_SUCCESS ){

    				H3_Attribute attrib = {.type = H3_ATTRIBUTE_PERMISSIONS, .mode = mode};
    				if(H3_SetObjectAttributes(data.handle, &data.token, data.bucket, directory, attrib) != H3_SUCCESS)
    					res = -ENOSPC;
    			}
    			else if(status == H3_NAME_TOO_LONG){
    				res = -ENAMETOOLONG;
    			}
    			else
    				res = -ENOSPC;
    		}
    		else {
    			free(objectNameArray);
    			res = -EEXIST;
    		}
    	}
		else if(status == H3_NAME_TOO_LONG){
			res = -ENAMETOOLONG;
		}

    	free(directory);
    }
    else
    	res = -EINVAL;

    return res;
}

static int H3FS_Unlink(const char* path){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length){
    	if(object[length] == '/'){
    		H3_ObjectInfo objectInfo;
    		switch ( H3_InfoObject(data.handle, &data.token, data.bucket, object, &objectInfo)){
    			case H3_SUCCESS: 		res = -EISDIR; break;
				case H3_NOT_EXISTS:  	res = -ENOENT; break;
				case H3_NAME_TOO_LONG: 	res = -ENAMETOOLONG; break;
				default: 				res = -EIO; break;
    		}
    	}
    	else
    		switch(H3_DeleteObject(data.handle, &data.token, data.bucket, object)){
    			case H3_SUCCESS:		res = 0; break;
				case H3_NOT_EXISTS:  	res = -ENOENT; break;
				case H3_NAME_TOO_LONG: 	res = -ENAMETOOLONG; break;
				default: 				res = -EIO; break;
    		}
    }
    else
    	res = -EISDIR;

    return res;
}

static int H3FS_RmDir(const char* path){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length){
        H3_Status status;
		H3_Name objectNameArray;
        H3_Name directory = NULL;
        uint32_t nObjects = 0;

        asprintf(&directory, "%s/", object);
		if((status = H3_ListObjects(data.handle, &data.token, data.bucket, directory, 0, &objectNameArray, &nObjects)) == H3_SUCCESS || status == H3_CONTINUE ){
			if(!nObjects){
				res = -ENOTDIR;
			}
			else if(nObjects == 1){
				if(H3_DeleteObject(data.handle, &data.token, data.bucket, directory) != H3_SUCCESS)
					res = -EINVAL;
			}
			else{
				res = -ENOTEMPTY;
			}
			free(objectNameArray);
		}
		else
			res = -EINVAL;
        free(directory);
    }
    else
    	res = -ENOTDIR;

    return res;
}

int ExamineObject(H3_Name object, char* isDir, char* isEmpty){
	int res = 0;
    H3_Status status;
    H3_Name objectNameArray;
    H3_Name directory = NULL;
    uint32_t nObjects = 0;

    asprintf(&directory, "%s/", object);
	if( (status = H3_ListObjects(data.handle, &data.token, data.bucket, directory, 0, &objectNameArray, &nObjects)) == H3_SUCCESS || status == H3_CONTINUE){
		if(nObjects > 1){
			*isDir = 1;
			*isEmpty = 0;
		}
		else if(nObjects == 1){
			*isDir = 1;
			*isEmpty = 1;
		}
		else {
			*isDir = 0;
		}

		res = 1;
		free(objectNameArray);
	}
    free(directory);

	return res;
}

static int H3FS_Rename(const char* srcPath, const char* dstPath, unsigned int flags){
	int res = 0;
	uint8_t noOverwrite = 0, swap;
	H3_Status status;
	H3_Name srcObject = (H3_Name)&srcPath[1];
	H3_Name dstObject = (H3_Name)&dstPath[1];
	size_t srcLength = strlen(srcObject);
	size_t dstLength = strlen(dstObject);

	switch(flags){
		case 0:
			swap = 0;
			noOverwrite = 0;
			break;

		case RENAME_NOREPLACE:
			swap = 0;
			noOverwrite = 1;
			break;

		case RENAME_EXCHANGE:
			swap = 1;
			break;

		default:
			return -EINVAL;
	}

	if(!srcLength || !dstLength){
		return -EINVAL;
	}

	// If one is directory and the other is a file
	char srcDir, srcEmpty, dstDir, dstEmpty = 1;
	if(!ExamineObject(srcObject, &srcDir, &srcEmpty) ||
	   !ExamineObject(dstObject, &dstDir, &dstEmpty)	){
		return -EINVAL;
	}

	if(srcDir && !dstDir)	return -ENOTDIR;
	if(!srcDir && dstDir)	return -EISDIR;
	if(!dstEmpty)			return -ENOTEMPTY;

	// Single file or empty 'directory'
	if(!srcDir || srcEmpty){
		if(!swap)
			status = H3_MoveObject(data.handle, &data.token, data.bucket, srcObject, dstObject, noOverwrite);
		else
			status = H3_ExchangeObject(data.handle, &data.token, data.bucket, srcObject, dstObject);

		switch(status){
			case H3_NOT_EXISTS: res = -ENOENT; break;
			case H3_FAILURE: if(swap) res = -ENOENT; else res = -EBADF; break;
			case H3_INVALID_ARGS: res = -EINVAL; break;
			case H3_EXISTS: res = -EEXIST; break;
			default: break;
		}
	}

	// A bunch of objects appearing as files in sub-directory
	else {
	    H3_Status status, moveStatus = H3_SUCCESS;
	    H3_Name objectNameArray;
	    uint32_t offset = 0, nObjects = 0;
	    while( (moveStatus == H3_SUCCESS) &&
	    	   ((status = H3_ListObjects(data.handle, &data.token, data.bucket, srcObject, offset, &objectNameArray, &nObjects)) == H3_CONTINUE ||
	    	    (status == H3_SUCCESS && nObjects)                                                                                                        ) ){

        	H3_Name src = objectNameArray;
        	char dst[H3_OBJECT_NAME_SIZE+1];
        	while(nObjects-- && moveStatus == H3_SUCCESS){
        		snprintf(dst, H3_OBJECT_NAME_SIZE, "%s%s", dstObject, &src[srcLength]);
        		if(!swap)
        			moveStatus = H3_MoveObject(data.handle, &data.token, data.bucket, src, dst, noOverwrite);
        		else
        			moveStatus = H3_ExchangeObject(data.handle, &data.token, data.bucket, src, dst);

        		src = &src[strlen(src)];
        	}

        	free(objectNameArray);
	    }

	    if(status != H3_SUCCESS || moveStatus != H3_SUCCESS)
	    	res = -EFAULT;
	}

	return res;
}


static int H3FS_ChMod(const char* path, mode_t mode, struct fuse_file_info* fi){
	int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length == 0){
    	res = -ENOENT;
    }
    else if(length > H3_OBJECT_NAME_SIZE){
    	res = -ENAMETOOLONG;
    }
    else{
    	H3_Attribute attrib = {.type = H3_ATTRIBUTE_PERMISSIONS, .mode = mode};
    	switch(H3_SetObjectAttributes(data.handle, &data.token, data.bucket, object, attrib)){
			case H3_SUCCESS:	res = 0; 		break;
			case H3_NOT_EXISTS:	res = -ENOENT;	break;
			case H3_FAILURE:	res = -EIO;		break;
			default:			res = -EINVAL; 	break;
    	}
    }

    if (res == -ENOENT) {
        // Check if this is a directory
        char isDir = 0, isEmpty = 0;
        if(!ExamineObject(object, &isDir, &isEmpty))
            return -EINVAL;

        if (isDir) {
            char *directory_path = NULL;
            asprintf(&directory_path, "/%s/", object);
            res = H3FS_ChMod(directory_path, mode, fi);
            free(directory_path);
        }
    }

    return res;
}

static int H3FS_ChOwn(const char* path, uid_t uid, gid_t gid, struct fuse_file_info* fi){
	int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length == 0){
    	return -ENOENT;
    }

	H3_Attribute attrib = {.type = H3_ATTRIBUTE_OWNER, .uid = uid, .gid = gid};
	switch(H3_SetObjectAttributes(data.handle, &data.token, data.bucket, object, attrib)){
		case H3_SUCCESS:		res = 0; 				break;
		case H3_NOT_EXISTS:		res = -ENOENT;			break;
		case H3_FAILURE:		res = -EIO;				break;
		case H3_NAME_TOO_LONG:	res = -ENAMETOOLONG;	break;
		default:				res = -EINVAL; 			break;
	}

    if (res == -ENOENT) {
        // Check if this is a directory
        char isDir = 0, isEmpty = 0;
        if(!ExamineObject(object, &isDir, &isEmpty))
            return -EINVAL;

        if (isDir) {
            char *directory_path = NULL;
            asprintf(&directory_path, "/%s/", object);
            res = H3FS_ChOwn(directory_path, uid, gid, fi);
            free(directory_path);
        }
    }

	return res;
}

static int H3FS_Truncate(const char* path, off_t size, struct fuse_file_info* fi){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length){
    	switch(H3_TruncateObject(data.handle, &data.token, data.bucket, object, size)){
			case H3_SUCCESS: 		res = 0; 				break;
			case H3_NOT_EXISTS: 	res = -ENOENT; 			break;
			case H3_NAME_TOO_LONG: 	res = -ENAMETOOLONG; 	break;
			case H3_FAILURE: 		res = -EIO; 			break;
			case H3_INVALID_ARGS:
			default:				res = -EINVAL; 			break;
    	}
    }
    else
        res = -ENOENT;

    return res;
}

static int H3FS_Open(const char* path , struct fuse_file_info* fi){
    return 0;
}

static int H3FS_Read(const char* path, char* buffer, size_t size, off_t offset , struct fuse_file_info* fi){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if (!length)
        return -ENOENT;

    do {
        void *buf = (void *)&(buffer[res]);
        size_t buf_size = size - res;
        switch(H3_ReadObject(data.handle, &data.token, data.bucket, object, offset + res, &buf, &buf_size)){
            case H3_SUCCESS:
                res += buf_size;
                return res;

            case H3_CONTINUE:
                res += buf_size;
                break;

            case H3_NOT_EXISTS:
                return -ENOENT;

            default:
                return -EINVAL;
        }
    } while (res >= 0 && res < size);

    return res;
}

static int H3FS_Write(const char* path, const char* buffer, size_t size, off_t offset, struct fuse_file_info* fi){
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length){
        switch(H3_WriteObject(data.handle, &data.token, data.bucket, object, (void*)buffer, size, offset)){
            case H3_SUCCESS:
                res = size;
                break;

            default:
                res = -EINVAL;
        }
    }
    else
        res = -ENOENT;

    return res;
}

static int H3FS_Flush(const char* path, struct fuse_file_info* fi){
	return 0;
}

static int H3FS_Release(const char* path, struct fuse_file_info* fi){
	return 0;
}

static int H3FS_Fsync(const char* path, int isDatasync, struct fuse_file_info* fi){
	return 0;
}

static int H3FS_ReadDir(const char* path, void* buffer, fuse_fill_dir_t filler, off_t fuseOffset, struct fuse_file_info* fi, enum fuse_readdir_flags flags){
    int res = 0;
    H3_Name prefix = (H3_Name)&path[1];
    size_t length = strlen(prefix);

    char *directory = NULL;
    if (length) {
       asprintf(&directory, "%s/", prefix);
    } else directory = "";

    int isDir = 0;
    GHashTable* uniqueDirEntries = g_hash_table_new(g_str_hash, g_str_equal);
    GPtrArray* objectArrays = g_ptr_array_new_full(20, free);

    uint32_t nObjects = 0;
    H3_Name objectNameArray;
    H3_Status status;
    uint32_t h3Offset = 0;
    while( (status = H3_ListObjects(data.handle, &data.token, data.bucket, directory, h3Offset, &objectNameArray, &nObjects)) == H3_CONTINUE || (status == H3_SUCCESS && nObjects)){
    	h3Offset += nObjects;
    	g_ptr_array_add(objectArrays, objectNameArray);

    	H3_Name nextEntry = &objectNameArray[strlen(objectNameArray) + 1];
    	H3_Name currentEntry = &objectNameArray[length];
    	while(nObjects--){
    		H3_Name dirEntry = Cast2DirEntry(currentEntry, &isDir);
    		g_hash_table_insert(uniqueDirEntries, dirEntry , GINT_TO_POINTER(isDir));
    		currentEntry = &nextEntry[length];
    		nextEntry = &nextEntry[strlen(nextEntry)+1];
    	}
    }

    if (length)
        free(directory);

    if(status != H3_SUCCESS){
    	res = -EINVAL;
    }
    else {
		// Use filler to populate the buffer
		struct stat st;
		memset(&st, 0, sizeof(st));
		filler(buffer, ".", NULL, 0, 0);
		filler(buffer, "..", NULL, 0, 0);

		GHashTableIter iter;
		gpointer key, value;

		g_hash_table_iter_init (&iter, uniqueDirEntries);
		while( g_hash_table_iter_next (&iter, &key, &value) ){
			if(fuseOffset){
				fuseOffset--;
				continue;
			}

			if(strlen((char*)key)){
				if(value){
					st.st_mode = S_IFDIR | 0755;
					st.st_nlink = 2;
					printf("Directory: %s\n", (char*)key);
				}
				else {
					st.st_mode = S_IFREG | 0777;
					st.st_nlink = 1;
					printf("File: %s\n", (char*)key);
				}

				if (filler(buffer, key, &st, 0, 0))
					break;
			}
		}
    }

    g_hash_table_destroy(uniqueDirEntries);
    g_ptr_array_free(objectArrays, TRUE);



    return res;
}

static void* H3FS_Init(struct fuse_conn_info* conn, struct fuse_config* cfg){
    (void) conn;
    (void) cfg;
//    conn->capable |= FUSE_CAP_NO_OPEN_SUPPORT;

    struct fuse_context* cxt = fuse_get_context();
    if(cxt) return cxt->private_data;

    return NULL;
}

static void H3FS_Destroy(void* privateData){
	H3_Free(data.handle);
}

static int H3FS_Access(const char* path, int mode){

    if(strcmp(path, "/") == 0){
    	if (access(path, mode) == -1)
    		return -errno;
    	else
    		return 0;
    }

    struct stat stbuf;
    return GetObjectInfo(path, &stbuf);
}

static int H3FS_Create(const char* path , mode_t mode, struct fuse_file_info* fi){
    int res = 0;
    H3_Status status;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    (void) fi;

	// A call to creat() is equivalent to calling open() with flags equal to O_CREAT|O_WRONLY|O_TRUNC
    if(length){
    	switch((status = H3_CreateObject(data.handle, &data.token, data.bucket, object, NULL, 0))){
			case H3_FAILURE:
			case H3_INVALID_ARGS: res = -EINVAL; break;
			case H3_EXISTS:
				if((status = H3_TruncateObject(data.handle, &data.token, data.bucket, object, 0)) != H3_SUCCESS){
					res = -EINVAL;
				}
				break;

			default: break;
    	}

    	H3_Attribute attrib = {.type = H3_ATTRIBUTE_PERMISSIONS, .mode = mode};
    	if(status == H3_SUCCESS && H3_SetObjectAttributes(data.handle, &data.token, data.bucket, object, attrib) != H3_SUCCESS){
    		res = -EINVAL;
    	}
    }
    else
    	res = -EISDIR;

    return res;
}

static int H3FS_Utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi) {
    int res = 0;
    H3_Name object = (H3_Name)&path[1];
    size_t length = strlen(object);

    if(length == 0){
        res = -ENOENT;
    }
    else if(length > H3_OBJECT_NAME_SIZE){
        res = -ENAMETOOLONG;
    }
    else{

        switch(H3_TouchObject(data.handle, &data.token, data.bucket, object, (struct timespec *)(tv != NULL ? &(tv[0]) : NULL), (struct timespec *)(tv != NULL ? &(tv[1]) : NULL))){
            case H3_SUCCESS:    res = 0;        break;
            case H3_NOT_EXISTS: res = -ENOENT;  break;
            case H3_FAILURE:    res = -EIO;     break;
            default:            res = -EINVAL;  break;
        }

        if (res == -ENOENT) {
            // Check if this is a directory
            char isDir = 0, isEmpty = 0;
            if(!ExamineObject(object, &isDir, &isEmpty))
                return -EINVAL;

            if (isDir) {
                char *directory_path = NULL;
                asprintf(&directory_path, "/%s/", object);
                res = H3FS_Utimens(directory_path, tv, fi);
                free(directory_path);
            }
        }
    }

    return res;
}

static ssize_t H3FS_CopyFileRange(const char* srcPath, struct fuse_file_info* srcFi, off_t srcOffset, const char* dstPath, struct fuse_file_info* dtsFi, off_t dstOffset, size_t size, int flags){
	ssize_t res = 0;
	H3_Name srcObject = (H3_Name)&srcPath[1];
	H3_Name dstObject = (H3_Name)&dstPath[1];
	size_t srcLength = strlen(srcObject);
	size_t dstLength = strlen(dstObject);

	if(flags){
		return -EINVAL;
	}

	if(!srcLength || !dstLength){
		return -EINVAL;
	}

	char srcDir, srcEmpty, dstDir, dstEmpty;
	if(!ExamineObject(srcObject, &srcDir, &srcEmpty) ||
	   !ExamineObject(dstObject, &dstDir, &dstEmpty)	){
		return -EINVAL;
	}

	if(srcDir || dstDir){
		return -EISDIR;
	}

	(void) srcFi;
	(void) dtsFi;

	switch(H3_WriteObjectCopy(data.handle, &data.token, data.bucket, srcObject, srcOffset, &size, dstObject, dstOffset)){
		case H3_SUCCESS: res = size; break;
		case H3_NOT_EXISTS: res = -EBADF; break;
		default: res = -EIO; break;
	}

	return res;
}

static struct fuse_operations h3fsOperations = {
    .getattr    		= H3FS_GetAttr,
//    NOT SUPPORTED - int (*readlink) (const char *, char *, size_t);
    .mknod				= H3FS_MkNod,
    .mkdir 				= H3FS_MkDir,
    .unlink				= H3FS_Unlink,
    .rmdir				= H3FS_RmDir,
//    NOT SUPPORTED - int (*symlink) (const char *, const char *);
    .rename				= H3FS_Rename,
//    NOT SUPPORTED - int (*link) (const char *, const char *);
    .chmod				= H3FS_ChMod,
	.chown				= H3FS_ChOwn,
	.truncate			= H3FS_Truncate,
    .open       		= H3FS_Open,
    .read       		= H3FS_Read,
    .write      		= H3FS_Write,
//    NOT NECESSARY - int (*statfs) (const char *, struct statvfs *);
    .flush				= H3FS_Flush,
    .release			= H3FS_Release,
    .fsync				= H3FS_Fsync,
//    NOT SUPPORTED - int (*setxattr) (const char *, const char *, const char *, size_t, int);
//    NOT SUPPORTED - int (*getxattr) (const char *, const char *, char *, size_t);
//    NOT SUPPORTED - int (*listxattr) (const char *, char *, size_t);
//    NOT SUPPORTED - int (*removexattr) (const char *, const char *);
//    NOT NECESSARY - int (*opendir) (const char *, struct fuse_file_info *);
    .readdir			= H3FS_ReadDir,
//    NOT NECESSARY - int (*releasedir) (const char *, struct fuse_file_info *);
//    NOT SUPPORTED - int (*fsyncdir) (const char *, int, struct fuse_file_info *);
    .init       		= H3FS_Init,
    .destroy			= H3FS_Destroy,
    .access				= H3FS_Access,
    .create				= H3FS_Create,
//    NOT SUPPORTED - int (*lock) (const char *, struct fuse_file_info *, int cmd, struct flock *);
    .utimens            = H3FS_Utimens,
//    NOT SUPPORTED - int (*bmap) (const char *, size_t blocksize, uint64_t *idx);
//    NOT SUPPORTED - int (*ioctl) (const char *, unsigned int cmd, void *arg, struct fuse_file_info *, unsigned int flags, void *data);
//    NOT SUPPORTED - int (*poll) (const char *, struct fuse_file_info *, struct fuse_pollhandle *ph, unsigned *reventsp);
//    NOT SUPPORTED - int (*write_buf) (const char *, struct fuse_bufvec *buf, off_t off, struct fuse_file_info *);
//    NOT SUPPORTED - int (*read_buf) (const char *, struct fuse_bufvec **bufp, size_t size, off_t off, struct fuse_file_info *);
//    NOT SUPPORTED - int (*flock) (const char *, struct fuse_file_info *, int op);
//    NOT SUPPORTED - int (*fallocate) (const char *, int, off_t, off_t, struct fuse_file_info *);
    .copy_file_range	= H3FS_CopyFileRange
//    NOT SUPPORTED - off_t (*lseek) (const char *, off_t off, int whence, struct fuse_file_info *);
};

#define H3FS_OPT(t, p, v) { t, offsetof(H3FS_Config, p), v }
static struct fuse_opt h3fsOptions[] = {
        H3FS_OPT("storage=%s",  storageUri,     0),
		H3FS_OPT("bucket=%s",   bucket,    		0),

        FUSE_OPT_KEY("-V",                      KEY_VERSION),
        FUSE_OPT_KEY("--version",               KEY_VERSION),
        FUSE_OPT_KEY("-h",                      KEY_HELP),
        FUSE_OPT_KEY("--help",                  KEY_HELP),
        FUSE_OPT_END
};


static int H3FS_OptionParser(void *data, const char *arg, int key, struct fuse_args *outargs) {
    switch (key) {
        case KEY_HELP:
                fprintf(stderr,"h3fs options:\n"
                               "    -o storage=STRING      storage URI\n"
                			   "    -o bucket=STRING       bucket name\n");
                fuse_opt_add_arg(outargs, "-h");
                fuse_main(outargs->argc, outargs->argv, &h3fsOperations, NULL);
                exit(1);

        case KEY_VERSION:
                fprintf(stderr, "h3fs v%d.%d using h3lib version %s\n", H3FS_VERSION_MAJOR, H3FS_VERSION_MINOR, H3_Version());
                fuse_opt_add_arg(outargs, "--version");
                fuse_main(outargs->argc, outargs->argv, &h3fsOperations, NULL);
                exit(0);
    }
    return 1;
}


int main(int argc, char *argv[]) {
    int ret = 0;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    H3FS_Config conf = {0};
    H3_BucketInfo info;


    // Allow full access to newly created files/directories
    umask(0);

    fuse_opt_parse(&args, &conf, h3fsOptions, H3FS_OptionParser);

    if(!conf.storageUri){
        fprintf(stderr, "Missing storage URI\n");
        exit(1);
    }

    if(!conf.bucket){
        fprintf(stderr, "Missing bucket name\n");
        exit(1);
    }

    // TODO: Replace the hard-coded token
    data.token.userId = 0;

    // H3lib cleanup will be handled by fuse.destroy
    data.bucket = strdup(conf.bucket);
    data.handle = H3_Init(conf.storageUri);
    if(H3_InfoBucket(data.handle, &data.token, data.bucket, &info, 0) == H3_SUCCESS){
        ret = fuse_main(args.argc, args.argv, &h3fsOperations, (void*)&data);
        fuse_opt_free_args(&args);
    }
    else {
    	fprintf(stderr, "Invalid bucket\n");
    	ret = -1;
    }

    return ret;
}


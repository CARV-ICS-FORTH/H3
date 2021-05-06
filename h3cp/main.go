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

package main

// #cgo CFLAGS: -I/usr/local/include
// #cgo LDFLAGS: -L/usr/local/lib -lh3lib
// #include <stdlib.h>
// #include "h3wrapper.h"
import "C"
import (
    "fmt"
    "flag"
    "os"
    "strings"
    "unsafe"
)

func parseH3Path(H3Path string) (string, string) {
    if strings.HasPrefix(H3Path, "h3://") {
        parts := strings.Split(H3Path[5:], "/")
        return parts[0], strings.Join(parts[1:], "/")
    }
    return "", H3Path
}

func usage() {
     fmt.Printf("Usage: %s -s <storage_uri> <src> <dst>\n", os.Args[0])
     flag.PrintDefaults()
}

func main() {
    storageUri := ""

    flag.Usage = usage
    flag.StringVar(&storageUri, "s", "", "H3 storage URI")
    flag.Parse()

    args := flag.Args()
    if storageUri == "" || len(args) != 2 {
        usage()
        return
    }

    srcBucket, srcPath := parseH3Path(args[0])
    dstBucket, dstPath := parseH3Path(args[1])
    if (srcBucket == "" && dstBucket == "") || (srcBucket != "" && dstBucket != "") {
        fmt.Printf("Error: Only one of the arguments must a path in H3 (starting with \"h3://\").\n")
        return
    }
    if strings.HasSuffix(srcPath, "/") || strings.HasSuffix(dstPath, "/") {
        fmt.Printf("Error: Please do not end an object with \"/\".\n")
        return
    }

    fmt.Println("Info: Using storage URI:", storageUri)
    storageUriStr := C.CString(storageUri)
    defer C.free(unsafe.Pointer(storageUriStr))
    handle := C.H3_Init(storageUriStr)
    defer C.H3_Free(handle)

    if srcBucket != "" {
        fmt.Println("Info: Read", srcPath, "from", srcBucket, "into", dstPath)

        bucketName := C.CString(srcBucket)
        defer C.free(unsafe.Pointer(bucketName))
        objectName := C.CString(srcPath)
        defer C.free(unsafe.Pointer(objectName))
        filename := C.CString(dstPath)
        defer C.free(unsafe.Pointer(filename))

        C.h3lib_read_object_to_file(handle, bucketName, objectName, filename) // XXX Check...
    } else if dstBucket != "" {
        fmt.Println("Info: Write", srcPath, "to", dstPath, "into", dstBucket)

        bucketName := C.CString(dstBucket)
        defer C.free(unsafe.Pointer(bucketName))
        objectName := C.CString(dstPath)
        defer C.free(unsafe.Pointer(objectName))
        filename := C.CString(srcPath)
        defer C.free(unsafe.Pointer(filename))

        C.h3lib_write_object_from_file(handle, bucketName, objectName, filename) // XXX Check...
    }
}

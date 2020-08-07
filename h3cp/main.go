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
    "path"
    "strings"
    "unsafe"
)

func getConfigPath() string {
    configFile := os.Getenv("H3_CONFIG")
    if configFile != "" {
        return configFile
    }

    wd, err := os.Getwd()
    if err == nil {
        configFile = path.Join(wd, "config.ini")
        stat, err := os.Stat(configFile)
        if err == nil {
            if !stat.IsDir() {
                return "config.ini"
            }
        }
    }

    home, err := os.UserHomeDir()
    if err == nil {
        return path.Join(home, ".h3", "config.ini")
    }

    return "config.ini"
}

func parseH3Path(H3Path string) (string, string) {
    if strings.HasPrefix(H3Path, "h3://") {
        parts := strings.Split(H3Path[5:], "/")
        return parts[0], strings.Join(parts[1:], "/")
    }
    return "", H3Path
}

func usage() {
     fmt.Printf("Usage: %s [-c <config>] <src> <dst>\n", os.Args[0])
     flag.PrintDefaults()
}

func main() {
    configFile := getConfigPath()

    flag.Usage = usage
    flag.StringVar(&configFile, "c", configFile, "H3 configuration file")
    flag.Parse()

    args := flag.Args()
    if len(args) != 2 {
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

    fmt.Println("Info: Using configuration file:", configFile)
    cfgFileName := C.CString(configFile)
    defer C.free(unsafe.Pointer(cfgFileName))
    handle := C.H3_Init(C.H3_STORE_CONFIG, cfgFileName) // XXX Check...
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

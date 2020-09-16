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

#ifndef UTIL_H_
#define UTIL_H_

#include <stdint.h>
#include <time.h>

// Use typeof to make sure each argument is evaluated only once
// https://gcc.gnu.org/onlinedocs/gcc-4.9.2/gcc/Typeof.html#Typeof
#define max(a,b) ({ __typeof__ (a) _a = (a); __typeof__ (b) _b = (b); _a > _b ? _a : _b; })
#define min(a,b) ({ __typeof__ (a) _a = (a); __typeof__ (b) _b = (b); _a < _b ? _a : _b; })

#define H3_MSG_SIZE     1024
#define H3_HEADER_SIZE  80

#ifdef DEBUG
#define LogActivity(level, ...) _LogActivity(level, __func__, __LINE__, __VA_ARGS__)
#else
#define LogActivity(level, ...)
#endif

#define __1KByte	1024
#define __1MByte	(1024 * __1KByte)
#define __1GByte	(1024 * __1MByte)

typedef enum{
    H3_INFO_MSG = 0,
    H3_DEBUG_MSG,
    H3_ERROR_MSG,
    H3_NumberOfLevels
}H3_MsgLevel;


void _LogActivity(H3_MsgLevel level, const char* function, int lineNumber, const char *format, ...);
int64_t Compare(struct timespec* a, struct timespec* b);
struct timespec Posterior(struct timespec* a, struct timespec* b);
struct timespec Anterior(struct timespec* a, struct timespec* b);
void* ReAllocFreeOnFail(void* buffer, size_t size);

#endif

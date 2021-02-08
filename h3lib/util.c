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
#include <string.h>
#include <stdarg.h>
#include <stdlib.h>
#include "util.h"

//    http://web.theurbanpenguin.com/adding-color-to-your-output-from-c/
static const char* color[] = {"\033[0;33m",     // H3_INFO  --> Yellow
                              "\033[0;32m",     // H3_DEBUG --> Green
                              "\033[1;31m",     // H3_ERROR --> Red
                              ""            };  // H3_NumberOfLevels

void _LogActivity(H3_MsgLevel level, const char* function, int lineNumber, const char *format, ...) {

    char buffer[H3_HEADER_SIZE + H3_MSG_SIZE];
    va_list arg;

    // Create header
    snprintf(buffer, H3_HEADER_SIZE, "%s%s @ %d - ", color[level], function, lineNumber);

    // Create message
    va_start(arg, format);
    vsnprintf(&buffer[strlen(buffer)], H3_MSG_SIZE - 10, format, arg);
    va_end(arg);

    // Print the message
    printf("%s\033[0m", buffer);
}

// res < 0	A < B
// res = 0	A == B
// res > 0  A > B
int64_t Compare(struct timespec* a, struct timespec* b){
	if(a->tv_sec == b->tv_sec)
		return a->tv_nsec - b->tv_nsec;

	return a->tv_sec - b->tv_sec;
}

struct timespec Posterior(struct timespec* a, struct timespec* b){
	return Compare(a,b)>0?*a:*b;
}

struct timespec Anterior(struct timespec* a, struct timespec* b){
	return Compare(a,b)<0?*a:*b;
}

void* ReAllocFreeOnFail(void* buffer, size_t size){
	void* tmp = realloc(buffer, size);
	if(!tmp){
		free(buffer);
	}

	return tmp;
}

#ifndef UTIL_H_
#define UTIL_H_


#define H3_MSG_SIZE     1024
#define H3_HEADER_SIZE  80

#ifdef DEBUG
#define LogActivity(level, ...) _LogActivity(level, __func__, __LINE__, __VA_ARGS__)
#else
#define LogActivity(level, ...)
#endif

typedef enum{
    H3_INFO_MSG = 0,
    H3_DEBUG_MSG,
    H3_ERROR_MSG,
    H3_NumberOfLevels
}H3_MsgLevel;


void _LogActivity(H3_MsgLevel level, const char* function, int lineNumber, const char *format, ...);


#endif

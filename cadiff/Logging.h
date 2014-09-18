//
//  Logging.h
//  cadiff
//
//  Created by Wade Tregaskis on 22/06/2014.
//  Copyright (c) 2014 Wade Tregaskis. All rights reserved.
//


#include <stdio.h>


extern int debugLoggingEnabled;

#define LOG_DEBUG(format, ...) ({ if (debugLoggingEnabled) { printf("Debug: " format, ## __VA_ARGS__); } })
#define LOG_WARNING(format, ...) ({ fflush(stdout); fprintf(stderr, "WARNING: " format, ## __VA_ARGS__); fflush(stderr); })
#define LOG_ERROR(format, ...) ({ fflush(stdout); fprintf(stderr, "ERROR: " format, ## __VA_ARGS__); fflush(stderr); })

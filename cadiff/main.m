//
//  main.m
//  cadiff
//
//  Created by Wade Tregaskis on 24/09/13.
//
// Copyright (c) 2013, Wade Tregaskis
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include <CommonCrypto/CommonDigest.h>

#import <Foundation/Foundation.h>


#define NOT_NULL(...) __attribute__((nonnull (__VA_ARGS__)))

static void usage(const char *invocationString) NOT_NULL(1) {
    printf("Usage: %s A B\n"
           "\n"
           "A and B are two files or two folders to compare.\n",
           invocationString);
}

static BOOL computeHashes(NSURL *files,
                          NSMutableDictionary *URLsToHashes,
                          NSMutableDictionary *hashesToURLs,
                          dispatch_queue_t updateQueue,
                          dispatch_group_t dispatchGroup) NOT_NULL(1, 2, 3) {
    NSDirectoryEnumerator *fileEnumerator
        = [NSFileManager.defaultManager enumeratorAtURL:files
                             includingPropertiesForKeys:nil
                                                options:NSDirectoryEnumerationSkipsHiddenFiles
                                           errorHandler:^(NSURL *url, NSError *error) {
        fprintf(stderr, "Error while enumerating files in \"%s\": %s\n", url.path.UTF8String, error.localizedDescription.UTF8String);
        return YES;
    }];

    if (!fileEnumerator) {
        fprintf(stderr, "Unable to enumerate files in \"%s\".\n", files.path.UTF8String);
        return NO;
    }

    BOOL allGood = YES;
    dispatch_semaphore_t concurrencyLimiter = dispatch_semaphore_create(4);
    dispatch_queue_t jobQueue = dispatch_queue_create("Job Queue", DISPATCH_QUEUE_SERIAL);

    for (NSURL *file in fileEnumerator) {
        dispatch_io_t fileIO = dispatch_io_create_with_path(DISPATCH_IO_STREAM,
                                                            file.path.UTF8String,
                                                            O_RDONLY | O_NOFOLLOW,
                                                            0,
                                                            dispatch_get_main_queue(), ^(int error) {
                                                                if (0 != error) {
                                                                    fprintf(stderr, "Error %d reading \"%s\".\n", error, file.path.UTF8String);
                                                                }
                                                            });

        if (!fileIO) {
            fprintf(stderr, "Unable to create I/O stream for \"%s\".\n", file.path.UTF8String);
            allGood = NO;
            break;
        }

        dispatch_io_set_high_water(fileIO, 1ULL << 20);
        dispatch_io_set_low_water(fileIO, 128ULL << 10);

        CC_SHA1_CTX *hashContext = malloc(sizeof(*hashContext));

        if (!hashContext) {
            fprintf(stderr, "Unable to allocate hash context (for \"%s\").\n", file.path.UTF8String);
            allGood = NO;
            break;
        }

        if (1 != CC_SHA1_Init(hashContext)) {
            fprintf(stderr, "Unable to initialise hash context (for \"%s\").\n", file.path.UTF8String);
            allGood = NO;
            free(hashContext);
            break;
        }

        dispatch_group_enter(dispatchGroup);
        dispatch_async(jobQueue, ^{
            dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

            dispatch_io_read(fileIO,
                             0,
                             SIZE_MAX,
                             dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_LOW, 0),
                             ^(bool done, dispatch_data_t data, int error) {
                                 if (0 == error) {
                                     dispatch_data_apply(data,
                                                         ^bool(dispatch_data_t region,
                                                               size_t offset,
                                                               const void *buffer,
                                                               size_t size) {
                                                             if (1 == CC_SHA1_Update(hashContext, buffer, (CC_LONG)size)) {
                                                                 return true;
                                                             } else {
                                                                 fprintf(stderr, "Error computing SHA1 on bytes [%zu, %zu] in \"%s\".\n", offset, offset + size - 1, file.path.UTF8String);
                                                                 return false;
                                                             }
                                                         });

                                     if (done) {
                                         unsigned char hash[CC_SHA1_DIGEST_LENGTH];

                                         if (1 == CC_SHA1_Final(hash, hashContext)) {
                                             NSData *hashAsData = [NSData dataWithBytes:hash length:sizeof(hash)];

                                             printf("Hash for \"%s\" is %s.\n", file.path.UTF8String, hashAsData.description.UTF8String);
                                             fflush(stdout);

                                             dispatch_sync(updateQueue, ^{
                                                 URLsToHashes[file] = hashAsData;
                                                 hashesToURLs[hashAsData] = file;
                                             });
                                         } else {
                                             fprintf(stderr, "Unable to conclude SHA1 of \"%s\".\n", file.path.UTF8String);
                                         }
                                     }
                                 } else {
                                     fprintf(stderr, "Error %d while reading from \"%s\".\n", error, file.path.UTF8String);
                                 }

                                 if (done) {
                                     free(hashContext);
                                     dispatch_group_leave(dispatchGroup);
                                     dispatch_semaphore_signal(concurrencyLimiter);
                                 }
                             });
        });
    }

    return allGood;
}

int main(int argc, char* const argv[]) {
    static const struct option longOptions[] = {
        {"help",    no_argument,        NULL, 'h'},
        {NULL,      0,                  NULL, 0}
    };

    int optionIndex = 0;
    while (-1 != (optionIndex = getopt_long(argc, argv, "h", longOptions, NULL))) {
        switch (optionIndex) {
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                fprintf(stderr, "Invalid arguments.\n");
                return EINVAL;
        }
    }
    const char *invocationString = argv[0];
    argc -= optind;
    argv += optind;

    if (2 != argc) {
        usage(invocationString);
        return EINVAL;
    }

    @autoreleasepool {
        NSURL *a = [NSURL fileURLWithPath:[[NSString stringWithUTF8String:argv[0]] stringByExpandingTildeInPath]];
        NSURL *b = [NSURL fileURLWithPath:[[NSString stringWithUTF8String:argv[1]] stringByExpandingTildeInPath]];

        NSMutableDictionary *aHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *bHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *aURLsToHashes = [NSMutableDictionary dictionary];
        NSMutableDictionary *bURLsToHashes = [NSMutableDictionary dictionary];

        dispatch_group_t dispatchGroup = dispatch_group_create();

        dispatch_queue_t updateQueue = dispatch_queue_create("Aggregation Queue", DISPATCH_QUEUE_SERIAL);

        if (!computeHashes(a, aURLsToHashes, aHashesToURLs, updateQueue, dispatchGroup)) {
            return -1;
        }

        if (!computeHashes(b, bURLsToHashes, bHashesToURLs, updateQueue, dispatchGroup)) {
            return -1;
        }

        dispatch_group_wait(dispatchGroup, DISPATCH_TIME_FOREVER);

        printf("Calculated %lu hashes for \"%s\", and %lu for \"%s\".\n",
               (unsigned long)aHashesToURLs.count,
               a.path.UTF8String,
               (unsigned long)bHashesToURLs.count,
               b.path.UTF8String);

        NSMutableDictionary *duplicates = [NSMutableDictionary dictionary];
        NSMutableSet *onlyInA = [NSMutableSet set];
        NSMutableSet *onlyInB = [NSMutableSet set];

        [aURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            NSURL *duplicateFile = bHashesToURLs[hash];

            if (duplicateFile) {
                printf("\"%s\" and \"%s\" have the same hash - comparing complete contents to be sure...\n", file.path.UTF8String, duplicateFile.path.UTF8String);
                fflush(stdout);

                // TODO: This is *horribly* slow (as in, uses a fraction of available disk bandwidth).  Ugh.  Find another way or just implement it directly.
                if ([NSFileManager.defaultManager contentsEqualAtPath:file.path andPath:duplicateFile.path]) {
                    duplicates[file] = duplicateFile;
                } else {
                    printf("False positive between \"%s\" and \"%s\".\n", file.path.UTF8String, duplicateFile.path.UTF8String);
                }
            } else {
                [onlyInA addObject:file];
            }
        }];

        [bURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            NSURL *duplicateFile = aHashesToURLs[hash];

            if (!duplicateFile) {
                [onlyInB addObject:file];
            }
        }];

        printf("\n");

        if (0 < duplicates.count) {
            printf("Duplicates:\n");

            [duplicates enumerateKeysAndObjectsUsingBlock:^(NSURL *aVersion, NSURL *bVersion, BOOL *stop) {
                printf("\t%s <-> %s\n", aVersion.path.UTF8String, bVersion.path.UTF8String);
            }];

            printf("\n");
        } else {
            printf("No duplicates.\n");
        }

        if (onlyInA.anyObject) {
            printf("Only in \"%s\":\n", a.path.UTF8String);

            [onlyInA enumerateObjectsUsingBlock:^(NSURL *file, BOOL *stop) {
                printf("\t%s\n", file.path.UTF8String);
            }];

            printf("\n");
        } else {
            printf("Nothing unique to \"%s\".\n", a.path.UTF8String);
        }

        if (onlyInB.anyObject) {
            printf("Only in \"%s\":\n", b.path.UTF8String);

            [onlyInB enumerateObjectsUsingBlock:^(NSURL *file, BOOL *stop) {
                printf("\t%s\n", file.path.UTF8String);
            }];

            printf("\n");
        } else {
            printf("Nothing unique to \"%s\".\n", b.path.UTF8String);
        }
    }

    return 0;
}

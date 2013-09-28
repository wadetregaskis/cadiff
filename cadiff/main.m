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
#include <sys/stat.h>

#include <CommonCrypto/CommonDigest.h>

#import <Foundation/Foundation.h>


// Flags
static int fBenchmark = NO;
static int fDebug = NO;
static int fVerify = NO;


#define LOG_DEBUG(format, ...) ({ if (fDebug) { printf(format, ## __VA_ARGS__); } })

#define NOT_NULL(...) __attribute__((nonnull (__VA_ARGS__)))

static void usage(const char *invocationString) NOT_NULL(1) {
    printf("Usage: %s A B\n"
           "\n"
           "A and B are two files or two folders to compare.\n",
           invocationString);
}

static dispatch_io_t openFile(NSURL *file) {
    dispatch_io_t fileIO = dispatch_io_create_with_path(DISPATCH_IO_STREAM,
                                                        file.path.UTF8String,
                                                        O_RDONLY | O_NOFOLLOW,
                                                        0,
                                                        dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0),
                                                        ^(int error) {
                                                            if (0 != error) {
                                                                fprintf(stderr, "Error %d (%s) reading \"%s\".\n", error, strerror(error), file.path.UTF8String);
                                                            }
                                                        });

    if (!fileIO) {
        fprintf(stderr, "Unable to create I/O stream for \"%s\".\n", file.path.UTF8String);
        return NULL;
    }

    dispatch_io_set_high_water(fileIO, 16ULL << 20);
    dispatch_io_set_low_water(fileIO, 128ULL << 10);

    return fileIO;
}

static void computeHashes(NSURL *files,
                          NSMutableDictionary *URLsToHashes,
                          NSMutableDictionary *hashesToURLs,
                          dispatch_semaphore_t concurrencyLimiter,
                          dispatch_queue_t syncQueue,
                          void (^completionBlock)(BOOL)) NOT_NULL(1, 2, 3) {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        __block BOOL allGood = YES;

        NSDirectoryEnumerator *fileEnumerator
            = [NSFileManager.defaultManager enumeratorAtURL:files
                                 includingPropertiesForKeys:nil
                                                    options:NSDirectoryEnumerationSkipsHiddenFiles
                                               errorHandler:^(NSURL *url, NSError *error) {
            fprintf(stderr, "Error while enumerating files in \"%s\": %s\n", url.path.UTF8String, error.localizedDescription.UTF8String);
            allGood = NO;
            return NO;
        }];

        if (!fileEnumerator) {
            fprintf(stderr, "Unable to enumerate files in \"%s\".\n", files.path.UTF8String);
            dispatch_async(syncQueue, ^{
                completionBlock(NO);
            });
            return;
        }

        dispatch_group_t dispatchGroup = dispatch_group_create();
        dispatch_queue_t jobQueue = dispatch_queue_create("Hash Job Queue", DISPATCH_QUEUE_SERIAL);

        for (NSURL *file in fileEnumerator) {
            if (!allGood) {
                break;
            }

            { // Skip folders (in the try-to-read-them-as-files sense; we will of course recurse into them to find files within.
                NSError *err = nil;
                NSNumber *isFolder = nil;

                if ([file getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err]) {
                    if ([isFolder boolValue]) {
                        LOG_DEBUG("Found subfolder \"%s\"...\n", file.path.UTF8String);
                        continue;
                    }
                } else {
                    fprintf(stderr, "Unable to determine if \"%s\" is a folder or not (assuming it's not), error: %s\n", file.path.UTF8String, err.localizedDescription.UTF8String);
                }
            }

            dispatch_io_t fileIO = openFile(file);

            if (!fileIO) {
                allGood = NO;
                break;
            }

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
                if (!allGood) {
                    return;
                }

                dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                dispatch_io_read(fileIO,
                                 0,
                                 SIZE_MAX,
                                 dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0),
                                 ^(bool done, dispatch_data_t data, int error) {
                                     if (!done && !allGood) {
                                         dispatch_io_close(fileIO, DISPATCH_IO_STOP);
                                         return;
                                     }

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
                                                                     allGood = NO;
                                                                     dispatch_io_close(fileIO, DISPATCH_IO_STOP);
                                                                     return false;
                                                                 }
                                                             });

                                         if (done) {
                                             unsigned char hash[CC_SHA1_DIGEST_LENGTH];

                                             if (1 == CC_SHA1_Final(hash, hashContext)) {
                                                 NSData *hashAsData = [NSData dataWithBytes:hash length:sizeof(hash)];

                                                 if (fDebug) {
                                                     LOG_DEBUG("Hash for \"%s\" is %s.\n", file.path.UTF8String, hashAsData.description.UTF8String);
                                                 } else {
                                                     printf("⎍"); fflush(stdout);
                                                 }

                                                 dispatch_async(syncQueue, ^{
                                                     URLsToHashes[file] = hashAsData;
                                                     hashesToURLs[hashAsData] = file;
                                                     dispatch_group_leave(dispatchGroup);
                                                 });
                                             } else {
                                                 fprintf(stderr, "Unable to conclude SHA1 of \"%s\".\n", file.path.UTF8String);
                                                 dispatch_group_leave(dispatchGroup);
                                             }
                                         }
                                     } else if (ECANCELED != error) {
                                         fprintf(stderr, "Error %d (%s) while reading from \"%s\".\n", error, strerror(error), file.path.UTF8String);
                                         allGood = NO;
                                     }

                                     if (done) {
                                         free(hashContext);
                                         dispatch_semaphore_signal(concurrencyLimiter);

                                         if (0 != error) {
                                             dispatch_group_leave(dispatchGroup);
                                         }
                                     }
                                 });
            });
        }

        dispatch_group_notify(dispatchGroup, syncQueue, ^{
            completionBlock(allGood);
        });
    });
}

static off_t sizeOfFile(NSURL *file) {
    struct stat stats;

    if (0 == lstat(file.path.UTF8String, &stats)) {
        return stats.st_size;
    } else {
        fprintf(stderr, "Unable to stat \"%s\", error #%d (%s).\n", file.path.UTF8String, errno, strerror(errno));
        return OFF_MIN;
    }
}

static BOOL compareFiles(NSURL *a, NSURL *b) {
    if ([a isEqual:b]) {
        return YES;
    }

    off_t aSize = sizeOfFile(a);

    if ((0 > aSize) || (aSize != sizeOfFile(b))) {
        return NO;
    }

    __block BOOL same = NO;

    dispatch_io_t aIO = openFile(a);

    if (aIO) {
        dispatch_io_t bIO = openFile(b);

        if (bIO) {
            dispatch_queue_t compareQueue = dispatch_queue_create("Compare Queue", DISPATCH_QUEUE_SERIAL);
            __block dispatch_data_t aData, bData;

            same = YES;
            dispatch_semaphore_t doneNotification = dispatch_semaphore_create(0);

            void (^ioHandler)(dispatch_data_t, BOOL, NSURL*) = ^(dispatch_data_t data, BOOL done, NSURL *file) {
                dispatch_async(compareQueue, ^{
                    if (file == a) {
                        aData = (aData ? dispatch_data_create_concat(aData, data) : data);
                    } else {
                        bData = (bData ? dispatch_data_create_concat(bData, data) : data);
                    }

                    const size_t aDataSize = (aData ? dispatch_data_get_size(aData) : 0);

                    if (0 < aDataSize) {
                        const size_t bDataSize = (bData ? dispatch_data_get_size(bData) : 0);

                        if (0 < bDataSize) {
                            __block size_t consumed = 0;

                            dispatch_data_apply(aData, ^bool(dispatch_data_t aRegion, size_t aOffset, const void *aBuffer, size_t aSize) {
                                dispatch_data_apply(bData, ^bool(dispatch_data_t bRegion, size_t bOffset, const void *bBuffer, size_t bSize) {
                                    if ((bOffset >= (aOffset + aSize)) || (aOffset >= (bOffset + bSize))) {
                                        return false;
                                    }

                                    const size_t aLocalOffset = bOffset - aOffset;
                                    const size_t size = MIN(aSize - aLocalOffset, bSize);

                                    if (0 == bcmp(aBuffer + aLocalOffset, bBuffer, size)) {
                                        consumed += size;
                                        return true;
                                    } else {
                                        dispatch_io_close(aIO, DISPATCH_IO_STOP);
                                        dispatch_io_close(bIO, DISPATCH_IO_STOP);
                                        same = NO;
                                        return false;
                                    }
                                });

                                if (same) {
                                    return (consumed >= aSize);
                                } else {
                                    return false;
                                }
                            });

                            if (same) {
                                assert((aDataSize == consumed) || (bDataSize == consumed));

                                aData = dispatch_data_create_subrange(aData, consumed, aDataSize - consumed);
                                bData = dispatch_data_create_subrange(bData, consumed, bDataSize - consumed);
                            }
                        }
                    }

                    if (done) {
                        dispatch_semaphore_signal(doneNotification);
                    }
                });
            };

            dispatch_io_read(aIO,
                             0,
                             SIZE_MAX,
                             dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_LOW, 0),
                             ^(bool done, dispatch_data_t data, int error) {
                                 if (0 == error) {
                                     dispatch_async(compareQueue, ^{
                                         ioHandler(data, done, a);
                                     });
                                 } else {
                                     if (ECANCELED != error) {
                                         fprintf(stderr, "Error %d (%s) while reading from \"%s\".\n", error, strerror(error), a.path.UTF8String);
                                     }
                                     dispatch_semaphore_signal(doneNotification);
                                 }
                             });

            dispatch_io_read(bIO,
                             0,
                             SIZE_MAX,
                             dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_LOW, 0),
                             ^(bool done, dispatch_data_t data, int error) {
                                 if (0 == error) {
                                     dispatch_async(compareQueue, ^{
                                         ioHandler(data, done, b);
                                     });
                                 } else {
                                     if (ECANCELED != error) {
                                         fprintf(stderr, "Error %d (%s) while reading from \"%s\".\n", error, strerror(error), b.path.UTF8String);
                                     }
                                     dispatch_semaphore_signal(doneNotification);
                                 }
                             });

            dispatch_semaphore_wait(doneNotification, DISPATCH_TIME_FOREVER);
        } else {
            dispatch_io_close(aIO, DISPATCH_IO_STOP);
        }
    }

    if (fVerify) {
        assert(same == [NSFileManager.defaultManager contentsEqualAtPath:a.path andPath:b.path]);
    }

    return same;
}

int main(int argc, char* const argv[]) {
    static const struct option longOptions[] = {
        {"benchmark",   no_argument,    &fBenchmark,    YES},
        {"debug",       no_argument,    &fDebug,        YES},
        {"help",        no_argument,    NULL,           'h'},
        {"verify",      no_argument,    &fVerify,       YES},
        {NULL,          0,              NULL,           0}
    };

    int optionIndex = 0;
    while (-1 != (optionIndex = getopt_long(argc, argv, "h", longOptions, NULL))) {
        switch (optionIndex) {
            case 0:
                // One of our boolean flags, that sets the global variable directly.  All good.
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                fprintf(stderr, "Invalid arguments (%d).\n", optionIndex);
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

    if (fBenchmark) {
        printf("Benchmark mode - disk caches will be purged before each major step.\n");
    }

    if (fVerify) {
        printf("File comparison algorithm will be verified by using a known-good (but slower) method too.\n");
    }

    if (fBenchmark) {
        assert(0 == system("/usr/bin/purge"));
    }

    @autoreleasepool {
        NSURL *a = [NSURL fileURLWithPath:[[NSString stringWithUTF8String:argv[0]] stringByExpandingTildeInPath]];
        NSURL *b = [NSURL fileURLWithPath:[[NSString stringWithUTF8String:argv[1]] stringByExpandingTildeInPath]];

        NSMutableDictionary *aHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *bHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *aURLsToHashes = [NSMutableDictionary dictionary];
        NSMutableDictionary *bURLsToHashes = [NSMutableDictionary dictionary];

        dispatch_semaphore_t concurrencyLimiter = dispatch_semaphore_create(8);
        dispatch_queue_t syncQueue = dispatch_queue_create("Sync Queue", DISPATCH_QUEUE_SERIAL);

        printf("Indexing"); fflush(stdout);

        dispatch_semaphore_t aHashingDone = dispatch_semaphore_create(0);
        dispatch_semaphore_t bHashingDone = dispatch_semaphore_create(0);
        __block BOOL successful = YES;

        computeHashes(a, aURLsToHashes, aHashesToURLs, concurrencyLimiter, syncQueue, ^(BOOL allGood) {
            if (!allGood) {
                successful = allGood;
            }

            dispatch_semaphore_signal(aHashingDone);
        });

        computeHashes(b, bURLsToHashes, bHashesToURLs, concurrencyLimiter, syncQueue, ^(BOOL allGood) {
            if (!allGood) {
                successful = allGood;
            }

            dispatch_semaphore_signal(bHashingDone);
        });

        dispatch_semaphore_wait(aHashingDone, DISPATCH_TIME_FOREVER);
        dispatch_semaphore_wait(bHashingDone, DISPATCH_TIME_FOREVER);

        if (!successful) {
            return -1;
        }

        printf("\n");

        LOG_DEBUG("Calculated %lu hashes for \"%s\", and %lu for \"%s\".\n",
                  (unsigned long)aHashesToURLs.count,
                  a.path.UTF8String,
                  (unsigned long)bHashesToURLs.count,
                  b.path.UTF8String);

        NSMutableDictionary *duplicates = [NSMutableDictionary dictionary];
        NSMutableSet *onlyInA = [NSMutableSet set];
        NSMutableSet *onlyInB = [NSMutableSet set];

        if (fBenchmark) {
            assert(0 == system("/usr/bin/purge"));
        }

        printf("Comparing suspects"); fflush(stdout);

        dispatch_group_t dispatchGroup = dispatch_group_create();

        [aURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            NSURL *duplicateFile = bHashesToURLs[hash];

            if (duplicateFile) {
                LOG_DEBUG("Verifying duplicity of \"%s\" and \"%s\"...\n", file.path.UTF8String, duplicateFile.path.UTF8String);

                dispatch_group_enter(dispatchGroup);

                dispatch_async(syncQueue, ^{
                    dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
                        if (compareFiles(file, duplicateFile)) {
                            printf("▲"); fflush(stdout);

                            dispatch_async(syncQueue, ^{
                                duplicates[file] = duplicateFile;
                                dispatch_group_leave(dispatchGroup);
                            });
                        } else {
                            if (fDebug) {
                                LOG_DEBUG("False positive between \"%s\" and \"%s\".\n", file.path.UTF8String, duplicateFile.path.UTF8String);
                            } else {
                                printf("△"); fflush(stdout);
                            }

                            dispatch_group_leave(dispatchGroup);
                        }

                        dispatch_semaphore_signal(concurrencyLimiter);
                    });
                });
            } else {
                [onlyInA addObject:file];
            }
        }];

        dispatch_group_wait(dispatchGroup, DISPATCH_TIME_FOREVER);

        [bURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            NSURL *duplicateFile = aHashesToURLs[hash];

            if (!duplicateFile) {
                [onlyInB addObject:file];
            }
        }];

        printf("\n\n");

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

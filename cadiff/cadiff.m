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
#define LOG_ERROR(format, ...) ({ fprintf(stderr, format, ## __VA_ARGS__); fflush(stderr); })

#define NOT_NULL(...) __attribute__((nonnull (__VA_ARGS__)))

static void usage(const char *invocationString) NOT_NULL(1) {
    printf("Usage: %s [FLAGS] A B\n"
           "\n"
           "A and B are two files or two folders to compare.\n"
           "\n"
           "Flags:\n"
           "\t--benchmark\t\t\"Benchmark\" mode, where file system caches are purged prior to each main step (to make successive runs more consistent and better show how much real I/O is being performed).\n"
           "\t--debug\t\t\tOutput additional logging, intended for debugging.\n"
           "\t--hashInputSizeLimit\tThe maximum number of bytes to use from each file for computing its hash.  Smaller values make the \"indexing\" stage go faster, and are thus good for working with very many files or where most files are not duplicates, but increases the risk of encountering a hash collision, which will abort the program.  Defaults to 1 MiB.\n"
           "\t--help\t\t\tPrint this usage information and exit.\n"
           "\t--verify\t\tVerify the final file comparison using an additional, slower-but-known-good method.  This is in addition to the normal, fast-but-more-complicated method.  Generally this has little performance impact, if you have sufficient free memory to cache recently compared files.\n",
           invocationString);
}

static dispatch_io_t openFile(NSURL *file, dispatch_semaphore_t concurrencyLimiter) {
    dispatch_io_t fileIO = dispatch_io_create_with_path(DISPATCH_IO_STREAM,
                                                        file.path.UTF8String,
                                                        O_RDONLY | O_NOFOLLOW | O_NONBLOCK,
                                                        0,
                                                        dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0),
                                                        ^(int error) {
                                                            if (0 != error) {
                                                                LOG_ERROR("Error %d (%s) reading \"%s\".\n", error, strerror(error), file.path.UTF8String);
                                                            }

                                                            if (concurrencyLimiter) {
                                                                dispatch_semaphore_signal(concurrencyLimiter);
                                                            }
                                                        });

    if (!fileIO) {
        LOG_ERROR("Unable to create I/O stream for \"%s\".\n", file.path.UTF8String);
        return NULL;
    }

    dispatch_io_set_high_water(fileIO, 16ULL << 20);
    dispatch_io_set_low_water(fileIO, 128ULL << 10);

    return fileIO;
}

static void computeHashes(NSURL *files,
                          size_t hashInputSizeLimit,
                          NSMutableDictionary *URLsToHashes,
                          NSMutableDictionary *hashesToURLs,
                          dispatch_queue_t syncQueue,
                          void (^completionBlock)(BOOL)) NOT_NULL(1, 3, 4) {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        __block BOOL allGood = YES;

        NSNumber *isFolder;
        NSError *err;
        id fileEnumerator;

        if ([files getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err] || !isFolder) {
            if (![isFolder boolValue]) {
                fileEnumerator = @[files];
            }
        } else {
            LOG_ERROR("Unable to determine if \"%s\" is a folder or a file.  Assuming it's a folder.  Specific error was: %s\n", files.path.UTF8String, err.localizedDescription.UTF8String);
        }

        if (!fileEnumerator) {
            fileEnumerator = [NSFileManager.defaultManager enumeratorAtURL:files
                                                includingPropertiesForKeys:nil
                                                                   options:NSDirectoryEnumerationSkipsHiddenFiles
                                                              errorHandler:^(NSURL *url, NSError *error) {
                LOG_ERROR("Error while enumerating files in \"%s\": %s\n", url.path.UTF8String, error.localizedDescription.UTF8String);
                allGood = NO;
                return NO;
            }];

            if (!fileEnumerator) {
                LOG_ERROR("Unable to enumerate files in \"%s\".\n", files.path.UTF8String);
                dispatch_async(syncQueue, ^{
                    completionBlock(NO);
                });
                return;
            }
        }

        dispatch_semaphore_t concurrencyLimiter = dispatch_semaphore_create(4);
        dispatch_group_t dispatchGroup = dispatch_group_create();
        dispatch_queue_t jobQueue = dispatch_queue_create([@"Hash Job Queue for " stringByAppendingString:files.path].UTF8String, DISPATCH_QUEUE_SERIAL);

        for (NSURL *file in fileEnumerator) {
            if (!allGood) {
                break;
            }

            { // Skip folders (in the try-to-read-them-as-files sense; we will of course recurse into them to find files within.
                if ([file getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err]) {
                    if ([isFolder boolValue]) {
                        LOG_DEBUG("Found subfolder \"%s\"...\n", file.path.UTF8String);
                        continue;
                    }
                } else {
                    LOG_ERROR("Unable to determine if \"%s\" is a folder or not (assuming it's not), error: %s\n", file.path.UTF8String, err.localizedDescription.UTF8String);
                }
            }

            dispatch_io_t fileIO = openFile(file, concurrencyLimiter);

            if (!fileIO) {
                allGood = NO;
                break;
            }

            dispatch_group_enter(dispatchGroup);
            dispatch_async(jobQueue, ^{
                CC_SHA1_CTX *hashContext;

                if (allGood) {
                    hashContext = malloc(sizeof(*hashContext));

                    if (hashContext) {
                        if (1 != CC_SHA1_Init(hashContext)) {
                            LOG_ERROR("Unable to initialise hash context (for \"%s\").\n", file.path.UTF8String);
                            allGood = NO;
                            free(hashContext);
                        }
                    } else {
                        LOG_ERROR("Unable to allocate hash context (for \"%s\").\n", file.path.UTF8String);
                        allGood = NO;
                    }
                }

                if (!allGood) {
                    dispatch_group_leave(dispatchGroup);
                    return;
                }

                dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                dispatch_io_read(fileIO,
                                 0,
                                 ((0 < hashInputSizeLimit) ? hashInputSizeLimit : SIZE_MAX),
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
                                                                     LOG_ERROR("Error computing SHA1 on bytes [%zu, %zu] in \"%s\".\n", offset, offset + size - 1, file.path.UTF8String);
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

                                                     NSMutableSet *existingEntry = hashesToURLs[hashAsData];

                                                     if (existingEntry) {
                                                         NSMutableString *errorMessage = [NSMutableString stringWithFormat:@"Hash collision between \"%@\" and: ", file.path];

                                                         for (NSURL *otherFile in existingEntry) {
                                                             [errorMessage appendFormat:@"\"%@\" ", otherFile.path];
                                                         }

                                                         LOG_ERROR("%s\n", errorMessage.UTF8String);

                                                         [existingEntry addObject:file];
                                                     } else {
                                                         hashesToURLs[hashAsData] = [NSMutableSet setWithObject:file];
                                                     }

                                                     dispatch_group_leave(dispatchGroup);
                                                 });
                                             } else {
                                                 LOG_ERROR("Unable to conclude SHA1 of \"%s\".\n", file.path.UTF8String);
                                                 dispatch_group_leave(dispatchGroup);
                                             }
                                         }
                                     } else {
                                         if (ECANCELED != error) {
                                             LOG_ERROR("Error %d (%s) while reading from \"%s\".\n", error, strerror(error), file.path.UTF8String);
                                             dispatch_io_close(fileIO, DISPATCH_IO_STOP);  // I feel like this should be redundant, but everyone else seems to be dispatch_io_closing on error, religiously.  So I've joined the cult.
                                         }

                                         allGood = NO;
                                     }

                                     if (done) {
                                         free(hashContext);

                                         // There's a delay between here and when the actual file descriptor is released.  It's an undefined delay - it depends on various queues' activity etc.  It's a pain because if we were to signal concurrencyLimiter here, it'd try to use another file descriptor potentially sooner than we release this one.  Repeat enough times and you run out of file descriptors.  So the obvious thing to do is call dispatch_io_close() right here.  That does in fact address that particular problem.  But it also, in the case where (0 == error && done), triggers a use-after-free bug in libdispatch (io.c:1261 in the source currently on libdispatch.macosforge.org).  It appears to be a genuine bug in libdispatch (rdar://problem/15109142), with no direct workaround I can find.  So the indirect workaround is to wait until the actual "close" block is run, in order to signal concurrencyLimiter.  And that was setup by openFile() at dispatch_io_t-creation time.

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

static off_t sizeOfFile(NSURL *file) NOT_NULL(1) {
    struct stat stats;

    if (0 == lstat(file.path.UTF8String, &stats)) {
        return stats.st_size;
    } else {
        LOG_ERROR("Unable to stat \"%s\", error #%d (%s).\n", file.path.UTF8String, errno, strerror(errno));
        return OFF_MIN;
    }
}

static BOOL compareFiles(NSURL *a, NSURL *b) NOT_NULL(1, 2) {
    if ([a isEqual:b]) {
        return YES;
    }

    off_t aSize = sizeOfFile(a);

    if ((0 > aSize) || (aSize != sizeOfFile(b))) {
        return NO;
    }

    __block BOOL same = NO;

    dispatch_io_t aIO = openFile(a, NULL);

    if (aIO) {
        dispatch_io_t bIO = openFile(b, NULL);

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
                                         LOG_ERROR("Error %d (%s) while reading from \"%s\".\n", error, strerror(error), a.path.UTF8String);
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
                                         LOG_ERROR("Error %d (%s) while reading from \"%s\".\n", error, strerror(error), b.path.UTF8String);
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

static void addValueToKey(NSMutableDictionary *dictionary, id key, id value) NOT_NULL(1, 2, 3) {
    NSMutableSet *existingEntry = dictionary[key];

    if (existingEntry) {
        [existingEntry addObject:value];
    } else {
        dictionary[key] = [NSMutableSet setWithObject:value];
    }
}

static NSComparisonResult compareURLs(NSURL *a, NSURL *b, void *unused) NOT_NULL(1, 2) {
    return [a.path compare:b.path options:(NSCaseInsensitiveSearch | NSAnchoredSearch | NSNumericSearch | NSDiacriticInsensitiveSearch | NSWidthInsensitiveSearch)];
}

static NSString* prettyFormatURLSet(NSSet *set) NOT_NULL(1) {
    if (0 == set.count) {
        return @"(empty)";
    } else if (1 == set.count) {
        return ((NSURL*)set.anyObject).path;
    } else {
        NSArray *sortedURLs = [[set allObjects] sortedArrayUsingFunction:compareURLs context:NULL];
        NSMutableString *result = [@"(" mutableCopy];
        BOOL haveFirst = NO;

        for (NSURL *URL in sortedURLs) {
            if (haveFirst) {
                [result appendFormat:@", \"%@\"", URL.path];
            } else {
                [result appendFormat:@"\"%@\"", URL.path];
                haveFirst = YES;
            }
        }

        [result appendString:@")"];

        return result;
    }
}

int main(int argc, char* const argv[]) NOT_NULL(2) {
    static const struct option longOptions[] = {
        {"benchmark",           no_argument,        &fBenchmark,    YES},
        {"debug",               no_argument,        &fDebug,        YES},
        {"hashInputSizeLimit",  required_argument,  NULL,           1},
        {"help",                no_argument,        NULL,           'h'},
        {"verify",              no_argument,        &fVerify,       YES},
        {NULL,                  0,                  NULL,           0}
    };

    size_t hashInputSizeLimit = 1ULL << 20;

    int optionIndex = 0;
    while (-1 != (optionIndex = getopt_long(argc, argv, "h", longOptions, NULL))) {
        switch (optionIndex) {
            case 0:
                // One of our boolean flags, that sets the global variable directly.  All good.
                break;
            case 1: {
                char *end = NULL;
                hashInputSizeLimit = strtoull(optarg, &end, 0);

                if (!end || *end) {
                    LOG_ERROR("Invalid hash input size limit \"%s\" - must be a positive number (or zero).\n", optarg);
                    return EINVAL;
                }

                break;
            }
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                LOG_ERROR("Invalid arguments (%d).\n", optionIndex);
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

        dispatch_queue_t syncQueue = dispatch_queue_create("Sync Queue", DISPATCH_QUEUE_SERIAL);

        printf("Indexing"); fflush(stdout);

        dispatch_semaphore_t aHashingDone = dispatch_semaphore_create(0);
        dispatch_semaphore_t bHashingDone = dispatch_semaphore_create(0);
        __block BOOL successful = YES;

        computeHashes(a, hashInputSizeLimit, aURLsToHashes, aHashesToURLs, syncQueue, ^(BOOL allGood) {
            if (!allGood) {
                successful = allGood;
            }

            dispatch_semaphore_signal(aHashingDone);
        });

        computeHashes(b, hashInputSizeLimit, bURLsToHashes, bHashesToURLs, syncQueue, ^(BOOL allGood) {
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
                  (unsigned long)aURLsToHashes.count,
                  a.path.UTF8String,
                  (unsigned long)bURLsToHashes.count,
                  b.path.UTF8String);

        NSMutableDictionary *aDuplicates = [NSMutableDictionary dictionary];
        NSMutableDictionary *bDuplicates = [NSMutableDictionary dictionary];
        NSMutableOrderedSet *onlyInA = [NSMutableOrderedSet orderedSet];
        NSMutableOrderedSet *onlyInB = [NSMutableOrderedSet orderedSet];

        if (fBenchmark) {
            assert(0 == system("/usr/bin/purge"));
        }

        printf("Comparing suspects"); fflush(stdout);

        dispatch_group_t dispatchGroup = dispatch_group_create();
        dispatch_semaphore_t concurrencyLimiter = dispatch_semaphore_create(4);

        [aURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            NSSet *potentialDuplicates = bHashesToURLs[hash];

            if (potentialDuplicates) {
                for (NSURL *potentialDuplicate in potentialDuplicates) {
                    LOG_DEBUG("Verifying duplicity of \"%s\" and \"%s\"...\n", file.path.UTF8String, potentialDuplicate.path.UTF8String);

                    dispatch_group_enter(dispatchGroup);
                    dispatch_async(syncQueue, ^{
                        dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                        dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
                            if (compareFiles(file, potentialDuplicate)) {
                                printf("▲"); fflush(stdout);

                                dispatch_async(syncQueue, ^{
                                    addValueToKey(aDuplicates, file, potentialDuplicate);
                                    addValueToKey(bDuplicates, potentialDuplicate, file);
                                    dispatch_group_leave(dispatchGroup);
                                });
                            } else {
                                if (fDebug) {
                                    LOG_DEBUG("False positive between \"%s\" and \"%s\".\n", file.path.UTF8String, potentialDuplicate.path.UTF8String);
                                } else {
                                    printf("△"); fflush(stdout);
                                }

                                dispatch_group_leave(dispatchGroup);
                            }

                            dispatch_semaphore_signal(concurrencyLimiter);
                        });
                    });
                }
            }
        }];

        dispatch_group_wait(dispatchGroup, DISPATCH_TIME_FOREVER);

        for (NSURL *file in aURLsToHashes) {
            if (!aDuplicates[file]) {
                [onlyInA addObject:file];
            }
        }

        for (NSURL *file in bURLsToHashes) {
            if (!bDuplicates[file]) {
                [onlyInB addObject:file];
            }
        }

        printf("\n\n");

        if (0 < aDuplicates.count) {
            NSMutableDictionary *duplicatePairs = [NSMutableDictionary dictionary];

            while (0 < aDuplicates.count) {
                __block NSURL *lastProcessed;

                [aDuplicates enumerateKeysAndObjectsUsingBlock:^(NSURL *a, NSSet *bSide, BOOL *stop) {
                    lastProcessed = a;

                    NSMutableSet *aSide = [NSMutableSet set];

                    for (NSURL *b in bSide) {
                        [aSide unionSet:bDuplicates[b]];
                        [bDuplicates removeObjectForKey:b];
                    }

                    if (0 < aSide.count) {
                        duplicatePairs[aSide] = bSide;
                    }

                    *stop = YES;
                }];

                [aDuplicates removeObjectForKey:lastProcessed];
            }

            printf("Duplicates:\n");

            NSArray *sortedURLs = [duplicatePairs.allKeys sortedArrayWithOptions:NSSortConcurrent usingComparator:^NSComparisonResult(NSSet *aSide, NSSet *bSide) {
                return [prettyFormatURLSet(aSide) compare:prettyFormatURLSet(bSide)
                                                  options:(NSCaseInsensitiveSearch | NSAnchoredSearch | NSNumericSearch | NSDiacriticInsensitiveSearch | NSWidthInsensitiveSearch)];
            }];

            [sortedURLs enumerateObjectsUsingBlock:^(NSSet *aSide, NSUInteger index, BOOL *stop) {
                printf("\t%s <-> %s\n", prettyFormatURLSet(aSide).UTF8String, prettyFormatURLSet((NSSet*)duplicatePairs[aSide]).UTF8String);
            }];

            printf("\n");
        } else {
            printf("No duplicates.\n");
        }

        NSComparisonResult (^URLComparator)(NSURL*, NSURL*) = ^NSComparisonResult(NSURL *a, NSURL *b) {
            return [a.path compare:b.path
                           options:(NSCaseInsensitiveSearch | NSAnchoredSearch | NSNumericSearch | NSDiacriticInsensitiveSearch | NSWidthInsensitiveSearch)];
        };

        if (0 < onlyInA.count) {
            printf("Only in \"%s\":\n", a.path.UTF8String);

            [onlyInA sortWithOptions:NSSortConcurrent usingComparator:URLComparator];
            [onlyInA enumerateObjectsUsingBlock:^(NSURL *file, NSUInteger index, BOOL *stop) {
                printf("\t%s\n", file.path.UTF8String);
            }];

            printf("\n");
        } else {
            printf("Nothing unique to \"%s\".\n", a.path.UTF8String);
        }

        if (0 < onlyInB.count) {
            printf("Only in \"%s\":\n", b.path.UTF8String);

            [onlyInB sortWithOptions:NSSortConcurrent usingComparator:URLComparator];
            [onlyInB enumerateObjectsUsingBlock:^(NSURL *file, NSUInteger index, BOOL *stop) {
                printf("\t%s\n", file.path.UTF8String);
            }];

            printf("\n");
        } else {
            printf("Nothing unique to \"%s\".\n", b.path.UTF8String);
        }
    }

    return 0;
}

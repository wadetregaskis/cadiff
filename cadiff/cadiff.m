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
#include <CoreGraphics/CoreGraphics.h>

#import <Foundation/Foundation.h>

#import "Logging.h"
#import "SSD.h"


// Declare a 'hidden' libdispatch function that we need to use to disable gratuitous read-ahead.
void _dispatch_iocntl(uint32_t param, uint64_t value);

// Likewise declare internal constants to use with the above function.
#define DISPATCH_IOCNTL_CHUNK_PAGES 1
#if TARGET_OS_EMBEDDED
#define DIO_MAX_CHUNK_PAGES				128u //  512kB chunk size
#else
#define DIO_MAX_CHUNK_PAGES				256u // 1024kB chunk size
#endif


// Flags
static int fBenchmark = NO;
static unsigned long fSSDConcurrencyLimit = 64; // SATA NCQ has a limit of ~32 outstanding I/Os to the actual drive.  So one might assume we need only pick something larger-enough than this to account for any overhead and additional queuing in the OS.
static unsigned long fSpindleConcurrencyLimitForSmallReads = 16;
static unsigned long fSpindleConcurrencyLimitForLargeReads = 1;
static int fVerify = NO;


#define NOT_NULL(...) __attribute__((nonnull (__VA_ARGS__)))


#ifndef countof
#define countof(x) (sizeof(x) / sizeof(*x))
#endif


NSNumberFormatter *decimalFormatter = nil;


NSMutableDictionary *volumeIsSSDCache = nil;


static void usage(const char *invocationString) NOT_NULL(1) {
    // This deliberately doesn't include all flags (e.g. those for concurrency limits) because such flags are really only intended for debugging, benchmarking, etc.
    printf("Usage: %s [FLAGS] A B\n"
           "\n"
           "A and B are two files or two folders to compare.\n"
           "\n"
           "Flags:\n"
           "\t--benchmark\t\t\"Benchmark\" mode, where file system caches are purged prior to each main step (to make successive runs more consistent and better show how much real I/O is being performed).\n"
           "\t--debug\t\t\tOutput additional logging, intended for debugging.\n"
           "\t--hashInputSizeLimit\tThe maximum number of bytes to use from each file for computing its hash.  Smaller values make the \"indexing\" stage go faster, and are thus good for working with very many files or where most files are not duplicates, but increases the risk of encountering a hash collision, which will abort the program.  Defaults to 1 MiB.\n"
           "\t--help\t\t\tPrint this usage information and exit.\n"
           "\t--showDuplicates BOOL\tWhether or not to show the full list of duplicates in the results.\n"
           "\t--showLeftUniques BOOL\tWhether or not to show the full list of files unique to A, in the results.\n"
           "\t--showRightUniques BOOL\tWhether or not to show the full list of files unique to B, in the results.\n"
           "\t--verify\t\tVerify the final file comparison using an additional, slower-but-known-good method.  This is in addition to the normal, fast-but-more-complicated method.  Generally this has little performance impact, if you have sufficient free memory to cache recently compared files.\n"
           "\t--visualCompare\tCompare images for visual equivalence, not byte-level equivalence.\n",
           invocationString);
}

static BOOL isFileOnSSD(NSURL *file) {
    BOOL result = YES; // Assume yes by default, i.e. in cases where we cannot determine it.

    struct stat fileStat;
    if (0 == stat(file.path.UTF8String, &fileStat)) {
        NSNumber *devAsNumber = @(fileStat.st_dev);

        if (volumeIsSSDCache[devAsNumber]) {
            result = ((NSNumber*)volumeIsSSDCache[devAsNumber]).boolValue;
        } else {
            if (isSolidState(fileStat.st_dev, &result)) {
                volumeIsSSDCache[devAsNumber] = @(result);
            } else {
                LOG_ERROR("Unable to determine whether or not the file \"%s\" is backed by an SSD.  Assuming it is.\n", file.path.UTF8String);
            }
        }

        LOG_DEBUG("File \"%s\" on volume (%u, %u) is %sliving on an SSD.\n",
                  file.path.UTF8String,
                  major(fileStat.st_dev),
                  minor(fileStat.st_dev),
                  (result ? "" : "not "));
    } else {
        LOG_ERROR("Unable to determine the volume dev# of \"%s\" (in order to optimise I/Os to it), error: (%d) %s\n", file.path.UTF8String, errno, strerror(errno));
    }

    return result;
}

static dispatch_io_t openFile(NSURL *file, size_t expectedExtentOfReading, BOOL cache, dispatch_semaphore_t concurrencyLimiter, BOOL *unsupportedFileType) {
    const int fd = open(file.path.UTF8String, O_RDONLY | O_NOFOLLOW | O_NONBLOCK);

    *unsupportedFileType = NO;

    if (0 > fd) {
        if (ELOOP == errno) {
            // The file is a symlink.  For now let's just ignore symlinks.  Perhaps in future we'll allow following of symlinks as long as they stay within the designated comparison directory, or something smarter like that.
            *unsupportedFileType = YES;
            return NULL;
        } else {
            LOG_ERROR("Unable to open \"%s\", error #%d (%s).\n", file.path.UTF8String, errno, strerror(errno));
            return NULL;
        }
    }

    if (!cache) {
        {
            const int err = fcntl(fd, F_RDAHEAD, 0);

            if (-1 == err) {
                LOG_WARNING("Unable to disable read-ahead of \"%s\", error #%d (%s).\n", file.path.UTF8String, errno, strerror(errno));
            }
        }

        {
            const int err = fcntl(fd, F_NOCACHE, 1);

            if (-1 == err) {
                LOG_WARNING("Unable to disable caching of \"%s\", error #%d (%s).\n", file.path.UTF8String, errno, strerror(errno));
            }
        }
    }

    dispatch_io_t fileIO = dispatch_io_create(DISPATCH_IO_STREAM,
                                              fd,
                                              dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0),
                                              ^(int error) {
                                                  if (0 != error) {
                                                      LOG_ERROR("Error %d (%s) reading \"%s\".\n", error, strerror(error), file.path.UTF8String);
                                                  }

                                                  const int err = close(fd);

                                                  if (0 != err) {
                                                      LOG_WARNING("Unable to close file descriptor %s (for \"%s\"), error #%d (%s).\n",
                                                                  [decimalFormatter stringFromNumber:@(fd)].UTF8String,
                                                                  file.path.UTF8String,
                                                                  errno, strerror(errno));
                                                  }

                                                  if (concurrencyLimiter) {
                                                      LOG_DEBUG("Signaling concurrency limiter %p from close handler for \"%s\".\n", concurrencyLimiter, file.path.UTF8String);
                                                      dispatch_semaphore_signal(concurrencyLimiter);
                                                  }
                                              });

    if (!fileIO) {
        LOG_ERROR("Unable to create I/O stream for \"%s\".\n", file.path.UTF8String);
        return NULL;
    }

    dispatch_io_set_high_water(fileIO, MIN(expectedExtentOfReading, 16ULL << 20));
    dispatch_io_set_low_water(fileIO, MIN(expectedExtentOfReading, 128ULL << 10));

    return fileIO;
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

static void recordHash(NSURL *file,
                       NSData *hash,
                       dispatch_queue_t syncQueue,
                       NSMutableDictionary *URLsToHashes,
                       NSMutableDictionary *hashesToURLs,
                       dispatch_group_t dispatchGroup,
                       NSInteger *hashesComputedSoFar) {
    if (debugLoggingEnabled) {
        LOG_DEBUG("Hash for \"%s\" is %s.\n", file.path.UTF8String, hash.description.UTF8String);
    }

    dispatch_async(syncQueue, ^{
        ++(*hashesComputedSoFar);

        URLsToHashes[file] = hash;

        NSMutableSet *existingEntry = hashesToURLs[hash];

        if (existingEntry) {
            NSMutableString *errorMessage = [NSMutableString stringWithFormat:@"Hash collision between \"%@\" and: ", file.path];

            for (NSURL *otherFile in existingEntry) {
                [errorMessage appendFormat:@"\"%@\" ", otherFile.path];
            }

            LOG_DEBUG("%s\n", errorMessage.UTF8String);

            [existingEntry addObject:file];
        } else {
            hashesToURLs[hash] = [NSMutableSet setWithObject:file];
        }

        dispatch_group_leave(dispatchGroup);
    });
}

static const NSDirectoryEnumerationOptions kDirectoryEnumerationOptions = NSDirectoryEnumerationSkipsHiddenFiles;

static void countCandidates(NSSet *fileURLs, dispatch_queue_t syncQueue, NSInteger *candidateCount) NOT_NULL(1, 3) {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        __block NSInteger countSoFar = 0;
        __block BOOL allGood = YES;

        for (NSURL *files in fileURLs) {
            NSNumber *isFolder;
            NSError *err;

            if ([files getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err] || !isFolder) {
                if (!isFolder.boolValue) {
                    ++countSoFar;
                    continue;
                }
            } else {
                LOG_ERROR("Unable to determine if \"%s\" is a folder or a file.  Assuming it's a folder.  Specific error was: %s\n", files.path.UTF8String, err.localizedDescription.UTF8String);
            }

            id fileEnumerator = [NSFileManager.defaultManager enumeratorAtURL:files
                                                   includingPropertiesForKeys:@[NSURLIsDirectoryKey]
                                                                      options:kDirectoryEnumerationOptions
                                                                 errorHandler:^(NSURL *url, NSError *error) {
                                                                     LOG_ERROR("Error while enumerating files in \"%s\": %s\n", url.path.UTF8String, error.localizedDescription.UTF8String);
                                                                     allGood = NO;
                                                                     return NO;
                                                                 }];

            if (!fileEnumerator) {
                LOG_ERROR("Unable to enumerate files in \"%s\".\n", files.path.UTF8String);
                return;
            }

            for (NSURL *file in fileEnumerator) {
                if (!allGood) {
                    return;
                }

                if ([file getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err]) {
                    if (!isFolder.boolValue) {
                        ++countSoFar;
                        continue;
                    }
                } else {
                    LOG_ERROR("Unable to determine if \"%s\" is a folder or not (assuming it's not), error: %s\n", file.path.UTF8String, err.localizedDescription.UTF8String);
                }
            }
        }

        dispatch_async(syncQueue, ^{
            *candidateCount = countSoFar;
        });
    });
}

static NSData* computeVisualHash(NSURL *file, size_t hashInputSizeLimit) NOT_NULL(1) {
    NSData *hash;
    NSDictionary *imageOptions = @{(__bridge NSString*)kCGImageSourceShouldAllowFloat: @YES,
                                   (__bridge NSString*)kCGImageSourceShouldCache: @NO};

    CGImageSourceRef imageSource = CGImageSourceCreateWithURL((__bridge CFURLRef)file, (__bridge CFDictionaryRef)imageOptions);

    if (imageSource) {
        CFDictionaryRef properties = CGImageSourceCopyPropertiesAtIndex(imageSource, 0, (__bridge CFDictionaryRef)imageOptions);

        if (properties) {
            CFNumberRef boxedWidth = CFDictionaryGetValue(properties, kCGImagePropertyPixelWidth);

            if (boxedWidth) {
                struct {
                    int64_t width;
                    int64_t height;
                } imageSize = {0};

                if (CFNumberGetValue(boxedWidth, kCFNumberSInt64Type, &imageSize.width)) {
                    CFNumberRef boxedHeight = CFDictionaryGetValue(properties, kCGImagePropertyPixelHeight);

                    if (boxedHeight) {
                        if (CFNumberGetValue(boxedHeight, kCFNumberSInt64Type, &imageSize.height)) {
                            if (0 < hashInputSizeLimit) {
                                NSDictionary *thumbnailOptions = @{(__bridge NSString*)kCGImageSourceShouldAllowFloat: @YES,
                                                                   (__bridge NSString*)kCGImageSourceCreateThumbnailFromImageAlways: @YES,
                                                                   (__bridge NSString*)kCGImageSourceThumbnailMaxPixelSize: @((size_t)sqrt(hashInputSizeLimit / 4)), // 4 channels, including alpha, at 8 bits each.
                                                                   (__bridge NSString*)kCGImageSourceCreateThumbnailWithTransform: @YES,
                                                                   (__bridge NSString*)kCGImageSourceShouldCache: @NO};

                                CGImageRef thumbnail = CGImageSourceCreateThumbnailAtIndex(imageSource, 0, (__bridge CFDictionaryRef)thumbnailOptions);

                                if (thumbnail) {
                                    CGColorSpaceRef colourSpace = CGColorSpaceCreateDeviceRGB();

                                    if (colourSpace) {
                                        CGContextRef bitmapContext = CGBitmapContextCreate(NULL, CGImageGetWidth(thumbnail), CGImageGetHeight(thumbnail), 8, 0, colourSpace, kCGImageAlphaPremultipliedLast);

                                        if (bitmapContext) {
                                            CGContextDrawImage(bitmapContext, CGRectMake(0, 0, CGImageGetWidth(thumbnail), CGImageGetHeight(thumbnail)), thumbnail);

                                            void* imageData = CGBitmapContextGetData(bitmapContext);

                                            if (imageData) {
                                                NSMutableData *mutableHash = [NSMutableData dataWithBytes:&imageSize length:sizeof(imageSize)];

                                                [mutableHash appendBytes:imageData length:CGBitmapContextGetBytesPerRow(bitmapContext) * CGBitmapContextGetHeight(bitmapContext)];

                                                hash = mutableHash;
                                            }

                                            CGContextRelease(bitmapContext);
                                        } else {
                                            LOG_ERROR("Unable to create bitmap context for thumbnail of \"%s\".\n", file.path.UTF8String);
                                        }

                                        CGColorSpaceRelease(colourSpace);
                                    } else {
                                        LOG_ERROR("Unable to create device-dependent RGB colour space (to use with \"%s\").", file.path.UTF8String);
                                    }

                                    CGImageRelease(thumbnail);
                                } else {
                                    LOG_ERROR("Unable to create thumbnail (as hash) for \"%s\".\n", file.path.UTF8String);
                                }
                            } else {
                                hash = [NSData dataWithBytes:&imageSize length:sizeof(imageSize)];
                            }
                        } else {
                            LOG_ERROR("Unable to interpret image width \"%s\", for \"%s\", as an integer.\n", ((__bridge NSObject*)boxedHeight).description.UTF8String, file.path.UTF8String);
                        }
                    } else {
                        LOG_ERROR("Unable to get image width of \"%s\".\n", file.path.UTF8String);
                    }
                } else {
                    LOG_ERROR("Unable to interpret image width \"%s\", for \"%s\", as an integer.\n", ((__bridge NSObject*)boxedWidth).description.UTF8String, file.path.UTF8String);
                }
            } else {
                LOG_ERROR("Unable to get image width of \"%s\".\n", file.path.UTF8String);
            }

            CFRelease(properties);
        } else {
            // Not an outright error because this is where you see the failure (as opposed to when creating the image source itself) when the source isn't a readable image.
            LOG_DEBUG("Unable to get the image properties of \"%s\".\n", file.path.UTF8String);
        }

        CFRelease(imageSource);
    } else {
        LOG_ERROR("Unable to create image source from \"%s\".\n", file.path.UTF8String);
    }

    return hash;
}

static void computeHashes(id files, // NSURL or a container (anything that responds to -(NSEnumerator)objectEnumerator) of NSURLs.
                          size_t hashInputSizeLimit,
                          NSMutableDictionary *URLsToHashes,
                          NSMutableDictionary *hashesToURLs,
                          BOOL visualCompare,
                          dispatch_queue_t syncQueue,
                          NSInteger *hashesComputedSoFar,
                          void (^completionBlock)(BOOL)) NOT_NULL(1, 3, 4) {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        __block BOOL allGood = YES;

        NSNumber *isFolder;
        NSError *err;
        id fileEnumerator;

        if ([files isKindOfClass:[NSURL class]]) {
            NSURL *filesURL = (NSURL*)files;

            if ([filesURL getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err] || !isFolder) {
                if (!isFolder.boolValue) {
                    fileEnumerator = @[filesURL];
                }
            } else {
                LOG_ERROR("Unable to determine if \"%s\" is a folder or a file.  Assuming it's a folder.  Specific error was: %s\n", filesURL.path.UTF8String, err.localizedDescription.UTF8String);
            }

            if (!fileEnumerator) {
                fileEnumerator = [NSFileManager.defaultManager enumeratorAtURL:filesURL
                                                    includingPropertiesForKeys:@[NSURLIsDirectoryKey, NSURLFileSizeKey]
                                                                       options:kDirectoryEnumerationOptions
                                                                  errorHandler:^(NSURL *url, NSError *error) {
                                                                      LOG_ERROR("Error while enumerating files in \"%s\": %s\n", url.path.UTF8String, error.localizedDescription.UTF8String);
                                                                      allGood = NO;
                                                                      return NO;
                                                                  }];

                if (!fileEnumerator) {
                    LOG_ERROR("Unable to enumerate files in \"%s\".\n", filesURL.path.UTF8String);
                    dispatch_async(syncQueue, ^{
                        completionBlock(NO);
                    });
                    return;
                }
            }
        } else {
            fileEnumerator = [files objectEnumerator];
        }

        dispatch_semaphore_t ssdConcurrencyLimiter = dispatch_semaphore_create(fSSDConcurrencyLimit);
        dispatch_semaphore_t spindleConcurrencyLimiter = dispatch_semaphore_create((4096 < hashInputSizeLimit) ? fSpindleConcurrencyLimitForLargeReads : fSpindleConcurrencyLimitForSmallReads);
        dispatch_group_t dispatchGroup = dispatch_group_create();
        dispatch_queue_t jobQueue = dispatch_queue_create([@"Hash Job Queue for " stringByAppendingString:[files description]].UTF8String, DISPATCH_QUEUE_SERIAL);

        for (NSURL *file in fileEnumerator) {
            if (!allGood) {
                break;
            }

            { // Skip folders (in the try-to-read-them-as-files sense; we will of course recurse into them to find files within.
                if ([file getResourceValue:&isFolder forKey:NSURLIsDirectoryKey error:&err]) {
                    if (isFolder.boolValue) {
                        LOG_DEBUG("Found subfolder \"%s\"...\n", file.path.UTF8String);
                        continue;
                    }
                } else {
                    LOG_ERROR("Unable to determine if \"%s\" is a folder or not (assuming it's not), error: %s\n", file.path.UTF8String, err.localizedDescription.UTF8String);
                }
            }

            // We use this to determine how aggressively to parallelise the file reading (and hashing) operations.  SSDs can handle concurrent operations much better than spindles (w.r.t. overall efficiency / throughput).
            // If we're not actually hashing the file, we don't need to worry about whether it's on an SSD or not since we won't be reading from it anyway.  We'll still be reading from the drive a little, to enumerate the directories and read file size metadata, but I suspect that's pretty much CPU- and/or kernel-bound most of the time regardless.
            const BOOL isOnSSD = (0 >= hashInputSizeLimit) || isFileOnSSD(file);
            dispatch_semaphore_t concurrencyLimiter = (isOnSSD ? ssdConcurrencyLimiter : spindleConcurrencyLimiter);

            dispatch_group_enter(dispatchGroup);
            dispatch_async(jobQueue, ^{
                dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                if (visualCompare) {
                    NSData *visualHashAsData = computeVisualHash(file, hashInputSizeLimit);

                    if (visualHashAsData) {
                        recordHash(file, visualHashAsData, syncQueue, URLsToHashes, hashesToURLs, dispatchGroup, hashesComputedSoFar);
                        LOG_DEBUG("Signaling concurrency limiter %p from visual hash 'hash'%s for \"%s\".\n", concurrencyLimiter, ((0 < hashInputSizeLimit) ? "" : " length only"), file.path.UTF8String);
                        dispatch_semaphore_signal(concurrencyLimiter);
                        return;
                    }
                }

                // If we reach this point, we weren't able to compute a visual hash for some reason.  So we'll fall back to just treating the file as an opaque binary blob.

                dispatch_io_t fileIO;
                CC_SHA1_CTX *hashContext = NULL;

                if (allGood && (0 < hashInputSizeLimit)) {
                    BOOL unsupportedFileType = NO;
                    fileIO = openFile(file, hashInputSizeLimit, NO, concurrencyLimiter, &unsupportedFileType);

                    if (fileIO) {
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
                    } else if (unsupportedFileType) {
                        LOG_DEBUG("Ignoring \"%s\" because it is a symlink.\n", file.path.UTF8String);
                        dispatch_group_leave(dispatchGroup);

                        LOG_DEBUG("Signaling concurrency limiter %p after file \"%s\" was discovered to be a symlink.", concurrencyLimiter, file.path.UTF8String);
                        dispatch_semaphore_signal(concurrencyLimiter);
                        return;
                    } else {
                        allGood = NO;
                    }
                }

                if (!allGood) {
                    dispatch_group_leave(dispatchGroup);

                    if (fileIO) {
                        dispatch_io_close(fileIO, DISPATCH_IO_STOP);
                    } else {
                        // If we successfully created the fileIO then its error handler, as will be called when it's closed, will signal concurrencyLimiter.  Until then we can't signal concurrencyLimiter as the file descriptor (that concurrencyLimiter primarily tries to bound simultaneous use of) will still be open, and we risk running out of file descriptors.
                        LOG_DEBUG("Signaling concurrency limiter %p from hash setup failure for \"%s\".\n", concurrencyLimiter, file.path.UTF8String);
                        dispatch_semaphore_signal(concurrencyLimiter);
                    }

                    return;
                }

                off_t fileSize = OFF_MIN;

                {
                    NSNumber *boxedFileSize;
                    NSError *err2;
                    if ([file getResourceValue:&boxedFileSize forKey:NSURLFileSizeKey error:&err2]) {
                        fileSize = boxedFileSize.unsignedLongLongValue;
                    } else {
                        LOG_ERROR("Unable to get size of \"%s\": %s.", file.path.UTF8String, err2.description.UTF8String);
                    }
                }

                NSData *fileSizeAsData = [NSData dataWithBytes:&fileSize length:sizeof(fileSize)];

                if (hashContext) {
                    dispatch_io_read(fileIO,
                                     0,
                                     hashInputSizeLimit,
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
                                                     NSMutableData *hashAsData = [fileSizeAsData mutableCopy];
                                                     [hashAsData appendBytes:hash length:sizeof(hash)];

                                                     recordHash(file, hashAsData, syncQueue, URLsToHashes, hashesToURLs, dispatchGroup, hashesComputedSoFar);
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
                } else {
                    recordHash(file, fileSizeAsData, syncQueue, URLsToHashes, hashesToURLs, dispatchGroup, hashesComputedSoFar);
                    LOG_DEBUG("Signaling concurrency limiter %p from 'hash' length only for \"%s\".\n", concurrencyLimiter, file.path.UTF8String);
                    dispatch_semaphore_signal(concurrencyLimiter);
                }
            });
        }

        dispatch_group_notify(dispatchGroup, syncQueue, ^{
            completionBlock(allGood);
        });
    });
}

static BOOL compareFiles(NSURL *a, NSURL *b) NOT_NULL(1, 2) {
    if ([a isEqual:b]) {
        return YES;
    }

    const off_t aSize = sizeOfFile(a);

    if ((0 > aSize) || (aSize != sizeOfFile(b))) {
        return NO;
    }

    __block BOOL same = NO;

    BOOL unsupportedFileType = NO;
    dispatch_io_t aIO = openFile(a, aSize, YES, NULL, &unsupportedFileType);

    if (aIO) {
        dispatch_io_t bIO = openFile(b, aSize, YES, NULL, &unsupportedFileType);

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
                                     ioHandler(data, done, a);
                                 } else {
                                     if (ECANCELED != error) {
                                         LOG_ERROR("Error %d (%s) while reading from \"%s\".\n", error, strerror(error), a.path.UTF8String);
                                     }
                                     dispatch_semaphore_signal(doneNotification);
                                 }

                                 if (done) {
                                     dispatch_io_close(aIO, DISPATCH_IO_STOP);
                                 }
                             });

            dispatch_io_read(bIO,
                             0,
                             SIZE_MAX,
                             dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_LOW, 0),
                             ^(bool done, dispatch_data_t data, int error) {
                                 if (0 == error) {
                                     ioHandler(data, done, b);
                                 } else {
                                     if (ECANCELED != error) {
                                         LOG_ERROR("Error %d (%s) while reading from \"%s\".\n", error, strerror(error), b.path.UTF8String);
                                     }
                                     dispatch_semaphore_signal(doneNotification);
                                 }

                                 if (done) {
                                     dispatch_io_close(bIO, DISPATCH_IO_STOP);
                                 }
                             });

            dispatch_semaphore_wait(doneNotification, DISPATCH_TIME_FOREVER);
        } else {
            if (unsupportedFileType) {
                LOG_ERROR("Don't know how to compare \"%s\" - it is an unsupported type of file.\n", b.path.UTF8String);
            }

            dispatch_io_close(aIO, DISPATCH_IO_STOP);
        }
    } else if (unsupportedFileType) {
        LOG_ERROR("Don't know how to compare \"%s\" - it is an unsupported type of file.\n", a.path.UTF8String);
    }

    if (fVerify) {
        assert(same == [NSFileManager.defaultManager contentsEqualAtPath:a.path andPath:b.path]);
    }

    return same;
}

static BOOL compareImages(CGImageRef imageA, CGImageRef imageB, NSURL *aURL, NSURL* bURL, BOOL *same) NOT_NULL(1, 2, 3, 4, 5) {
    BOOL successful = NO;

    if ((CGImageGetWidth(imageA) == CGImageGetWidth(imageB)) && (CGImageGetHeight(imageA) == (CGImageGetHeight(imageB)))) {
        CGColorSpaceRef colourSpaceA = CGImageGetColorSpace(imageA);
        CGColorSpaceRef colourSpaceB = CGImageGetColorSpace(imageB);

        CGColorSpaceRef colourSpace = NULL;

        if (colourSpaceA && colourSpaceB && CFEqual(colourSpaceA, colourSpaceB)) {
            colourSpace = CGColorSpaceRetain(colourSpaceA);
        } else {
            colourSpace = CGColorSpaceCreateDeviceRGB();
        }

        if (colourSpace) {
            const size_t supersetBitsPerComponent = MAX(CGImageGetBitsPerComponent(imageA), CGImageGetBitsPerComponent(imageB));
            LOG_DEBUG("supersetBitsPerComponent = %zu (max of %zu & %zu).\n", supersetBitsPerComponent, CGImageGetBitsPerComponent(imageA), CGImageGetBitsPerComponent(imageB));
            const CGImageAlphaInfo aAlphaInfo = CGImageGetAlphaInfo(imageA);
            const CGImageAlphaInfo bAlphaInfo = CGImageGetAlphaInfo(imageB);
            const BOOL alphaNeeded = (    (    (kCGImageAlphaNone != aAlphaInfo)
                                            && (kCGImageAlphaNoneSkipLast != aAlphaInfo)
                                            && (kCGImageAlphaNoneSkipFirst != aAlphaInfo))
                                       || (    (kCGImageAlphaNone != bAlphaInfo)
                                            && (kCGImageAlphaNoneSkipLast != bAlphaInfo)
                                            && (kCGImageAlphaNoneSkipFirst != bAlphaInfo)));
            const CGBitmapInfo supersetBitmapInfo = (   (kCGBitmapFloatComponents & (CGImageGetBitmapInfo(imageA) | CGImageGetBitmapInfo(imageB)))
                                                      | (alphaNeeded ? kCGImageAlphaPremultipliedLast : kCGImageAlphaNoneSkipLast));

            CGContextRef contextA = CGBitmapContextCreate(NULL, CGImageGetWidth(imageA), CGImageGetHeight(imageA), supersetBitsPerComponent, 0, colourSpace, supersetBitmapInfo);

            if (contextA) {
                CGContextRef contextB = CGBitmapContextCreate(NULL, CGImageGetWidth(imageB), CGImageGetHeight(imageB), supersetBitsPerComponent, 0, colourSpace, supersetBitmapInfo);

                if (contextB) {
                    CGContextDrawImage(contextA, CGRectMake(0, 0, CGImageGetWidth(imageA), CGImageGetHeight(imageA)), imageA);

                    const void *dataA = CGBitmapContextGetData(contextA);

                    if (dataA) {
                        CGContextDrawImage(contextB, CGRectMake(0, 0, CGImageGetWidth(imageB), CGImageGetHeight(imageB)), imageB);

                        const void *dataB = CGBitmapContextGetData(contextB);

                        if (dataB) {
                            *same = (0 == memcmp(dataA, dataB, CGBitmapContextGetBytesPerRow(contextA) * CGBitmapContextGetHeight(contextA)));
                            successful = YES;
                        } else {
                            LOG_ERROR("Unable to get the bitmap image data for \"%s\".\n", bURL.path.UTF8String);
                        }
                    } else {
                        LOG_ERROR("Unable to get the bitmap image data for \"%s\".\n", aURL.path.UTF8String);
                    }

                    CGContextRelease(contextB);
                } else {
                    LOG_ERROR("Unable to create image context for \"%s\".\n", bURL.path.UTF8String);
                }

                CGContextRelease(contextA);
            } else {
                LOG_ERROR("Unable to create image context for \"%s\".\n", aURL.path.UTF8String);
            }

            CGColorSpaceRelease(colourSpace);
        } else {
            LOG_ERROR("Unable to create device-dependent RGB colour space.\n");
        }
    } else {
        LOG_DEBUG("Images \"%s\" & \"%s\" are different as they have different sizes (%zu x %zu vs %zu x %zu, respectively).\n", aURL.path.UTF8String, bURL.path.UTF8String, CGImageGetWidth(imageA), CGImageGetHeight(imageA), CGImageGetWidth(imageB), CGImageGetHeight(imageB));
    }

    return successful;
}

static BOOL compareFilesVisually(NSURL *a, NSURL *b) NOT_NULL(1, 2) {
    if ([a isEqual:b]) {
        return YES;
    }

    BOOL same = NO;
    BOOL canCompareVisually = NO;

    NSDictionary *imageOptions = @{(__bridge NSImage*)kCGImageSourceShouldAllowFloat: @YES,
                                   (__bridge NSString*)kCGImageSourceShouldCache: @NO};

    CGImageSourceRef aImageSource = CGImageSourceCreateWithURL((__bridge CFURLRef)a, (__bridge CFDictionaryRef)imageOptions);

    if (aImageSource) {
        CGImageSourceRef bImageSource = CGImageSourceCreateWithURL((__bridge CFURLRef)b, (__bridge CFDictionaryRef)imageOptions);

        if (bImageSource) {
            CGImageRef aImage = CGImageSourceCreateImageAtIndex(aImageSource, 0, (__bridge CFDictionaryRef)imageOptions);

            if (aImage) {
                CGImageRef bImage = CGImageSourceCreateImageAtIndex(bImageSource, 0, (__bridge CFDictionaryRef)imageOptions);

                if (bImage) {
                    canCompareVisually = compareImages(aImage, bImage, a, b, &same);

                    CGImageRelease(bImage);
                } else {
                    LOG_DEBUG("Unable to create image from \"%s\".\n", b.path.UTF8String);
                }

                CGImageRelease(aImage);
            } else {
                LOG_DEBUG("Unable to create image from \"%s\".\n", a.path.UTF8String);
            }

            CFRelease(bImageSource);
        } else {
            LOG_DEBUG("Unable to create image source from \"%s\".\n", b.path.UTF8String);
        }

        CFRelease(aImageSource);
    } else {
        LOG_DEBUG("Unable to create image source from \"%s\".\n", a.path.UTF8String);
    }


    if (!canCompareVisually) {
        same = compareFiles(a, b);
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
        NSArray *sortedURLs = [set.allObjects sortedArrayUsingFunction:compareURLs context:NULL];
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

BOOL purge(void) {
    int err = system("/usr/sbin/purge");

    if (0 == err) {
        return YES;
    } else {
        LOG_DEBUG("Unable to run /usr/sbin/purge, error #%d (errno %d - %s).\n", err, errno, strerror(errno));

        err = system("/usr/bin/purge");

        if (0 == err) {
            return YES;
        } else {
            LOG_ERROR("Unable to run /usr/bin/purge, error #%d (errno %d - %s).\n", err, errno, strerror(errno));
        }
    }

    return NO;
}

NSString* formatTimeInterval(NSTimeInterval interval) {
    const int hours = (int)floor(interval / 3600);
    interval -= (hours * 3600);
    const int minutes = (int)floor(interval / 60);
    interval -= (minutes * 60);
    const int seconds = (int)floor(interval);

    if (0 < hours) {
        if (0 < minutes) {
            if (0 < seconds) {
                return [NSString stringWithFormat:@"%dh %dm %ds", hours, minutes, seconds];
            } else {
                return [NSString stringWithFormat:@"%dh %dm", hours, minutes];
            }
        } else if (0 < seconds) {
            return [NSString stringWithFormat:@"%dh %ds", hours, seconds];
        } else {
            return [NSString stringWithFormat:@"%dh", hours];
        }
    } else if (0 < minutes) {
        if (0 < seconds) {
            return [NSString stringWithFormat:@"%dm %ds", minutes, seconds];
        } else {
            return [NSString stringWithFormat:@"%dm", minutes];
        }
    } else {
        return [NSString stringWithFormat:@"%ds", seconds];
    }
}

NSString* estimatedTimeRemaining(double progress, NSDate *startTime) {
    if ((0 < progress) && (10 <= -startTime.timeIntervalSinceNow)) {
        return formatTimeInterval(-startTime.timeIntervalSinceNow * ((1 / progress) - 1));
    } else {
        return @"estimating time";
    }
}

void showHashProgress(const char *phaseName, NSInteger countSoFar, NSInteger total, NSDate *startTime) {
    NSString *ofTotalString;

    if (0 < total) {
        ofTotalString = [NSString stringWithFormat:@" (of %@)", [decimalFormatter stringFromNumber:@(total)]];
    } else {
        ofTotalString = @"";
    }

    NSString *timeRemainingString;

    if ((0 == total) || (countSoFar != total)) {
        timeRemainingString = [NSString stringWithFormat:@" - %@ remaining", estimatedTimeRemaining(((0 < total) ? (countSoFar / (double)total) : -1), startTime)];
    } else {
        timeRemainingString = @"";
    }

    printf("\33[2K\r%s... %s%s candidates scanned (in %s%s)",
           phaseName,
           [decimalFormatter stringFromNumber:@(countSoFar)].UTF8String,
           ofTotalString.UTF8String,
           formatTimeInterval(-startTime.timeIntervalSinceNow).UTF8String,
           timeRemainingString.UTF8String);
}

void showProgressBar(double progress, int *lastProgressPrinted, NSDate *startTime, NSDate **lastUpdateTime) {
    const int dotCount = (int)(progress * 100);

    if ((dotCount == *lastProgressPrinted) && ((nil == *lastUpdateTime) || (5 > -(*lastUpdateTime).timeIntervalSinceNow))) {
        return;
    }

    char dots[dotCount + 1];
    memset(dots, '*', sizeof(dots));
    dots[dotCount] = 0;

    printf("\33[2K\r %3d%% [%-100s] %s remaining",
           dotCount,
           dots,
           estimatedTimeRemaining(progress, startTime).UTF8String);

    *lastProgressPrinted = dotCount;
    *lastUpdateTime = [NSDate date];
}

BOOL computeHashesForBothSides(const char *phaseName,
                               id a, // NSURL or a container (anything that responds to -(NSEnumerator)objectEnumerator) of NSURLs.
                               id b, // NSURL or a container (anything that responds to -(NSEnumerator)objectEnumerator) of NSURLs.
                               size_t hashInputSizeLimit,
                               const NSInteger *candidateCount,
                               NSMutableDictionary *aHashesToURLs,
                               NSMutableDictionary *bHashesToURLs,
                               NSMutableDictionary *aURLsToHashes,
                               NSMutableDictionary *bURLsToHashes,
                               BOOL visualCompare,
                               dispatch_queue_t syncQueue) {
    printf("%s...", phaseName); fflush(stdout);

    NSDate *startTime = [NSDate date];
    __block BOOL successful = YES;
    __block NSInteger hashesComputedSoFar = 0;
    {
        dispatch_semaphore_t aHashingDone = dispatch_semaphore_create(0);
        dispatch_semaphore_t bHashingDone = dispatch_semaphore_create(0);

        // TODO:  'aHashesToURLs' isn't used anywhere else right now.  If it's not used after the N-way-compare feature is implemented, remove it (i.e. just pass nil, and update computeHashes() to silently ignore a nil argument).
        computeHashes(a, hashInputSizeLimit, aURLsToHashes, aHashesToURLs, visualCompare, syncQueue, &hashesComputedSoFar, ^(BOOL allGood) {
            if (!allGood) {
                successful = allGood;
            }

            dispatch_semaphore_signal(aHashingDone);
        });

        computeHashes(b, hashInputSizeLimit, bURLsToHashes, bHashesToURLs, visualCompare, syncQueue, &hashesComputedSoFar, ^(BOOL allGood) {
            if (!allGood) {
                successful = allGood;
            }

            dispatch_semaphore_signal(bHashingDone);
        });

        dispatch_semaphore_t doneSemaphores[] = {aHashingDone, bHashingDone};

        for (int i = 0; i < countof(doneSemaphores); ++i) {
            while (0 != dispatch_semaphore_wait(doneSemaphores[i], dispatch_time(DISPATCH_TIME_NOW, 333 * NSEC_PER_MSEC))) {
                showHashProgress(phaseName, hashesComputedSoFar, *candidateCount, startTime);
                fflush(stdout);
            }
        }
    }

    showHashProgress(phaseName, hashesComputedSoFar, *candidateCount, startTime);
    printf(".\n");

    return successful;
}

BOOL interpretAsBoolean(const char *input, BOOL *result) NOT_NULL(1, 2) {
    if (    (0 == strcasecmp(input, "true"))
         || (0 == strcasecmp(input, "yes"))
         || (0 == strcasecmp(input, "yep"))
         || (0 == strcasecmp(input, "sure"))
         || (0 == strcasecmp(input, "yargh"))) {
        *result = YES;
        return YES;
    } else if (    (0 == strcasecmp(input, "false"))
                || (0 == strcasecmp(input, "no"))
                || (0 == strcasecmp(input, "nope"))
                || (0 == strcasecmp(input, "nah"))
                || (0 == strcasecmp(input, "nargh"))) {
        *result = NO;
        return YES;
    } else {
        return NO;
    }
}

int main(int argc, char* const argv[]) NOT_NULL(2) {
    decimalFormatter = [[NSNumberFormatter alloc] init];
    decimalFormatter.numberStyle = NSNumberFormatterDecimalStyle;

    volumeIsSSDCache = [NSMutableDictionary dictionary];

    static int visualCompare = NO;

    static const struct option longOptions[] = {
        {"benchmark",               no_argument,        &fBenchmark,            YES},
        {"spindleConcurrencyLimit", required_argument,  NULL,                   2},
        {"ssdConcurrencyLimit",     required_argument,  NULL,                   3},
        {"debug",                   no_argument,        &debugLoggingEnabled,   YES},
        {"hashInputSizeLimit",      required_argument,  NULL,                   1},
        {"help",                    no_argument,        NULL,                   'h'},
        {"showDuplicates",          required_argument,  NULL,                   5},
        {"showLeftUniques",         required_argument,  NULL,                   6},
        {"showRightUniques",        required_argument,  NULL,                   7},
        {"verify",                  no_argument,        &fVerify,               YES},
        {"version",                 no_argument,        NULL,                   4},
        {"visualCompare",           no_argument,        &visualCompare,         YES},
        {NULL,                      0,                  NULL,                   0}
    };

    size_t hashInputSizeLimit = 512;
    BOOL showDuplicates = YES;
    BOOL showLeftUniques = YES;
    BOOL showRightUniques = YES;

    int optionIndex = 0;
    while (-1 != (optionIndex = getopt_long(argc, argv, "h", longOptions, NULL))) {
        switch (optionIndex) {
            case 0:
                // One of our boolean flags, that sets the global variable directly.  All good.
                break;
            case 1: { // --hashInputSizeLimit
                char *end = NULL;
                hashInputSizeLimit = strtoull(optarg, &end, 0);

                if (!end || *end) {
                    LOG_ERROR("Invalid hash input size limit \"%s\" - must be a positive number (or zero).\n", optarg);
                    return EINVAL;
                }

                break;
            }
            case 2: { // --spindleConcurrencyLimit
                char *end = NULL;
                fSpindleConcurrencyLimitForSmallReads = fSpindleConcurrencyLimitForLargeReads = strtoul(optarg, &end, 0);

                if (!end || *end || (1 > fSpindleConcurrencyLimitForSmallReads) || (1 > fSpindleConcurrencyLimitForLargeReads)) {
                    LOG_ERROR("Invalid spindle concurrency limit \"%s\" - must be a positive number.\n", optarg);
                    return EINVAL;
                }
                
                break;
            }
            case 3: { // --ssdConcurrencyLimit
                char *end = NULL;
                fSSDConcurrencyLimit = strtoul(optarg, &end, 0);

                if (!end || *end || (1 > fSSDConcurrencyLimit)) {
                    LOG_ERROR("Invalid SSD concurrency limit \"%s\" - must be a positive number.\n", optarg);
                    return EINVAL;
                }

                break;
            }
            case 4: // --version
                printf("Source version " __TIMESTAMP__ ".\n");
                printf("Built using " __VERSION__ " at " __TIME__ " on " __DATE__ ".\n");
                return 0;
            case 5: // --showDuplicates
                if (!interpretAsBoolean(optarg, &showDuplicates)) {
                    LOG_ERROR("Unable to interpret \"%s\" as yes or no - should be a boolean of some kind, e.g. \"yes\" or \"no\".\n", optarg);
                    return EINVAL;
                }

                break;
            case 6: // --showLeftUniques
                if (!interpretAsBoolean(optarg, &showLeftUniques)) {
                    LOG_ERROR("Unable to interpret \"%s\" as yes or no - should be a boolean of some kind, e.g. \"yes\" or \"no\".\n", optarg);
                    return EINVAL;
                }

                break;
            case 7: // --showRightUniques
                if (!interpretAsBoolean(optarg, &showRightUniques)) {
                    LOG_ERROR("Unable to interpret \"%s\" as yes or no - should be a boolean of some kind, e.g. \"yes\" or \"no\".\n", optarg);
                    return EINVAL;
                }

                break;
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
        assert(purge());
    }

    @autoreleasepool {
        NSURL *a = [NSURL fileURLWithPath:@(argv[0]).stringByExpandingTildeInPath];
        NSURL *b = [NSURL fileURLWithPath:@(argv[1]).stringByExpandingTildeInPath];

        NSMutableSet *aFilesWithInterestingSizes = [NSMutableSet set];
        NSMutableSet *bFilesWithInterestingSizes = [NSMutableSet set];

        dispatch_queue_t syncQueue = dispatch_queue_create("Sync Queue", DISPATCH_QUEUE_SERIAL);

        NSSet *aURLs, *bURLs;

        {
            NSMutableDictionary *aURLsToSizes = [NSMutableDictionary dictionary];
            NSMutableDictionary *bURLsToSizes = [NSMutableDictionary dictionary];
            NSMutableDictionary *aSizesToURLs = [NSMutableDictionary dictionary];
            NSMutableDictionary *bSizesToURLs = [NSMutableDictionary dictionary];
            NSInteger candidateCount = 0;
            countCandidates([NSSet setWithObjects:a, b, nil], syncQueue, &candidateCount);

            const BOOL successful = computeHashesForBothSides("Pre-scanning",
                                                              a,
                                                              b,
                                                              0,
                                                              &candidateCount,
                                                              aSizesToURLs,
                                                              bSizesToURLs,
                                                              aURLsToSizes,
                                                              bURLsToSizes,
                                                              visualCompare,
                                                              syncQueue);

            if (!successful) {
                return -1;
            }

            LOG_DEBUG("Fetched file sizes for %s files in \"%s\", and %s in \"%s\".\n",
                      [decimalFormatter stringFromNumber:@(aURLsToSizes.count)].UTF8String,
                      a.path.UTF8String,
                      [decimalFormatter stringFromNumber:@(bURLsToSizes.count)].UTF8String,
                      b.path.UTF8String);

            aURLs = [aURLsToSizes keysOfEntriesWithOptions:NSEnumerationConcurrent passingTest:^BOOL(id key, id obj, BOOL *stop) {
                return YES;
            }];

            bURLs = [bURLsToSizes keysOfEntriesWithOptions:NSEnumerationConcurrent passingTest:^BOOL(id key, id obj, BOOL *stop) {
                return YES;
            }];

            [aSizesToURLs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
                NSSet *aSideMatches = (NSSet*)obj;
                NSSet *bSideMatches = bSizesToURLs[key];

                // Compare if there's files on both sides with the same hash, *or* if either side has multiple matches (so that if nothing else we can highlight 'internal' duplicates, between two files on a single side).
                if (bSideMatches || (1 < aSideMatches.count)) {
                    [aFilesWithInterestingSizes unionSet:aSideMatches];
                    [bFilesWithInterestingSizes unionSet:bSideMatches];

                    [bSizesToURLs removeObjectForKey:key];
                }
            }];

            // We've now enumerated all the files that might actually match *between* A and B, but for consistency (re. showing 'internal' duplicates) we also need to still include any files on B's side that, while they clearly don't match any on A, might match each other.
            [bSizesToURLs enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
                NSSet *bSideMatches = (NSSet*)obj;

                if (1 < bSideMatches.count) {
                    [bFilesWithInterestingSizes unionSet:bSideMatches];
                }
            }];
        }

        NSMutableDictionary *aHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *bHashesToURLs = [NSMutableDictionary dictionary];
        NSMutableDictionary *aURLsToHashes = [NSMutableDictionary dictionary];
        NSMutableDictionary *bURLsToHashes = [NSMutableDictionary dictionary];

        {
            const NSInteger candidateCount = aFilesWithInterestingSizes.count + bFilesWithInterestingSizes.count;

            //printf("Scanning %lld potential duplicates...", (long long)candidateCount); fflush(stdout);

            _dispatch_iocntl(DISPATCH_IOCNTL_CHUNK_PAGES, MIN((unsigned int)ceil(hashInputSizeLimit / 512.0), DIO_MAX_CHUNK_PAGES));

            const BOOL successful = computeHashesForBothSides("Scanning",
                                                              aFilesWithInterestingSizes,
                                                              bFilesWithInterestingSizes,
                                                              hashInputSizeLimit,
                                                              &candidateCount,
                                                              aHashesToURLs,
                                                              bHashesToURLs,
                                                              aURLsToHashes,
                                                              bURLsToHashes,
                                                              visualCompare,
                                                              syncQueue);

            if (!successful) {
                return -1;
            }

            _dispatch_iocntl(DISPATCH_IOCNTL_CHUNK_PAGES, DIO_MAX_CHUNK_PAGES);

            LOG_DEBUG("Calculated %s hashes for \"%s\", and %s for \"%s\".\n",
                      [decimalFormatter stringFromNumber:@(aURLsToHashes.count)].UTF8String,
                      a.path.UTF8String,
                      [decimalFormatter stringFromNumber:@(bURLsToHashes.count)].UTF8String,
                      b.path.UTF8String);
        }

        NSMutableDictionary *aDuplicates = [NSMutableDictionary dictionary];
        NSMutableDictionary *bDuplicates = [NSMutableDictionary dictionary];
        NSMutableOrderedSet *onlyInA = [NSMutableOrderedSet orderedSet];
        NSMutableOrderedSet *onlyInB = [NSMutableOrderedSet orderedSet];

        if (fBenchmark) {
            assert(purge());
        }

        __block NSInteger totalSuspects = 0;

        [aURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
            totalSuspects += [bHashesToURLs[hash] count];  // Dot-syntax not convenient here because the target is of type id, and oddly dot-syntax requires foreknowledge of the method whereas a regular method invocation does not.
        }];

        if (0 < totalSuspects) {
            __block NSInteger suspectsAnalysedSoFar = 0;
            printf("Verifying %s suspected duplicates...\n", [decimalFormatter stringFromNumber:@(totalSuspects)].UTF8String); fflush(stdout);

            NSDate *startTime = [NSDate date];

            dispatch_group_t dispatchGroup = dispatch_group_create();
            dispatch_semaphore_t ssdConcurrencyLimiter = dispatch_semaphore_create(fSSDConcurrencyLimit);
            dispatch_semaphore_t spindleConcurrencyLimiter = dispatch_semaphore_create(fSpindleConcurrencyLimitForLargeReads);

            [aURLsToHashes enumerateKeysAndObjectsUsingBlock:^(NSURL *file, NSData *hash, BOOL *stop) {
                NSSet *potentialDuplicates = bHashesToURLs[hash];

                if (potentialDuplicates) {
                    for (NSURL *potentialDuplicate in potentialDuplicates) {
                        dispatch_group_enter(dispatchGroup);
                        dispatch_async(syncQueue, ^{
                            dispatch_semaphore_t concurrencyLimiter = ((isFileOnSSD(file) && isFileOnSSD(potentialDuplicate))
                                                                       ? ssdConcurrencyLimiter
                                                                       : spindleConcurrencyLimiter);

                            dispatch_semaphore_wait(concurrencyLimiter, DISPATCH_TIME_FOREVER);

                            LOG_DEBUG("Verifying duplicity of \"%s\" and \"%s\"...\n", file.path.UTF8String, potentialDuplicate.path.UTF8String);

                            dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
                                if (visualCompare ? compareFilesVisually(file, potentialDuplicate) : compareFiles(file, potentialDuplicate)) {
                                    dispatch_async(syncQueue, ^{
                                        addValueToKey(aDuplicates, file, potentialDuplicate);
                                        addValueToKey(bDuplicates, potentialDuplicate, file);
                                        dispatch_group_leave(dispatchGroup);
                                    });
                                } else {
                                    if (debugLoggingEnabled) {
                                        LOG_DEBUG("False positive between \"%s\" and \"%s\".\n", file.path.UTF8String, potentialDuplicate.path.UTF8String);
                                    }

                                    dispatch_group_leave(dispatchGroup);
                                }

                                ++suspectsAnalysedSoFar;
                                OSMemoryBarrier();
                                dispatch_semaphore_signal(concurrencyLimiter);
                            });
                        });
                    }
                }
            }];

            int lastProgressPrinted = -1;
            NSDate *lastUpdateTime;

            while (0 != dispatch_group_wait(dispatchGroup, dispatch_time(DISPATCH_TIME_NOW, 333 * NSEC_PER_MSEC))) {
                showProgressBar((double)suspectsAnalysedSoFar / totalSuspects, &lastProgressPrinted, startTime, &lastUpdateTime);
                fflush(stdout);
            }

            OSMemoryBarrier();
            showProgressBar((double)suspectsAnalysedSoFar / totalSuspects, &lastProgressPrinted, startTime, &lastUpdateTime);
            printf("\n");
        }

        for (NSURL *file in aURLs) {
            if (!aDuplicates[file]) {
                [onlyInA addObject:file];
            }
        }

        for (NSURL *file in bURLs) {
            if (!bDuplicates[file]) {
                [onlyInB addObject:file];
            }
        }

        printf("\n");

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

            if (showDuplicates) {
                printf("%lu set%s of duplicates:\n", (unsigned long)duplicatePairs.count, ((1 == duplicatePairs.count) ? "" : "s"));

                NSArray *sortedURLs = [duplicatePairs.allKeys sortedArrayWithOptions:NSSortConcurrent usingComparator:^NSComparisonResult(NSSet *aSide, NSSet *bSide) {
                    return [prettyFormatURLSet(aSide) compare:prettyFormatURLSet(bSide)
                                                      options:(NSCaseInsensitiveSearch | NSAnchoredSearch | NSNumericSearch | NSDiacriticInsensitiveSearch | NSWidthInsensitiveSearch)];
                }];

                [sortedURLs enumerateObjectsUsingBlock:^(NSSet *aSide, NSUInteger index, BOOL *stop) {
                    printf("\t%s <-> %s\n", prettyFormatURLSet(aSide).UTF8String, prettyFormatURLSet((NSSet*)duplicatePairs[aSide]).UTF8String);
                }];

                printf("\n");
            } else {
                printf("%lu set%s of duplicates.\n", (unsigned long)duplicatePairs.count, ((1 == duplicatePairs.count) ? "" : "s"));
            }
        } else {
            printf("No duplicates.\n");
        }

        NSComparisonResult (^URLComparator)(NSURL*, NSURL*) = ^NSComparisonResult(NSURL *a, NSURL *b) {
            return [a.path compare:b.path
                           options:(NSCaseInsensitiveSearch | NSAnchoredSearch | NSNumericSearch | NSDiacriticInsensitiveSearch | NSWidthInsensitiveSearch)];
        };

        if (0 < onlyInA.count) {
            printf("%lu unique item%s in \"%s\"", (unsigned long)onlyInA.count, ((1 == onlyInA.count) ? "" : "s"), a.path.UTF8String);

            if (showLeftUniques) {
                printf(":\n");

                [onlyInA sortWithOptions:NSSortConcurrent usingComparator:URLComparator];
                [onlyInA enumerateObjectsUsingBlock:^(NSURL *file, NSUInteger index, BOOL *stop) {
                    printf("\t%s\n", file.path.UTF8String);
                }];

                printf("\n");
            } else {
                printf(".\n");
            }
        } else {
            printf("Nothing unique to \"%s\".\n", a.path.UTF8String);
        }

        if (0 < onlyInB.count) {
            printf("%lu unique item%s in \"%s\"", (unsigned long)onlyInB.count, ((1 == onlyInB.count) ? "" : "s"), b.path.UTF8String);

            if (showRightUniques) {
                printf(":\n");

                [onlyInB sortWithOptions:NSSortConcurrent usingComparator:URLComparator];
                [onlyInB enumerateObjectsUsingBlock:^(NSURL *file, NSUInteger index, BOOL *stop) {
                    printf("\t%s\n", file.path.UTF8String);
                }];

                printf("\n");
            } else {
                printf(".\n");
            }
        } else {
            printf("Nothing unique to \"%s\".\n", b.path.UTF8String);
        }
    }

    return 0;
}

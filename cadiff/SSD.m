// Original version:  https://code.google.com/p/itunesfixer/source/browse/trunk/SSD.m?r=2

// Copyright (c) 2010, porneL
// Modified 2014, Wade Tregaskis.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#import "SSD.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <paths.h>
#include <sys/param.h>
#include <sys/stat.h>

#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/IOBSD.h>
#include <IOKit/IOTypes.h>
#include <IOKit/storage/IOBlockStorageDevice.h>
#include <IOKit/storage/IOMedia.h>
#include <IOKit/Kext/KextManager.h>


BOOL isSolidState(dev_t dev)
{
    io_iterator_t entryIterator;

    {
        CFMutableDictionaryRef classesToMatch = IOServiceMatching("IOMedia");

        if (classesToMatch) {
            CFDictionaryAddValue(classesToMatch, CFSTR("BSD Major"), (__bridge const void *)@(major(dev)));
            CFDictionaryAddValue(classesToMatch, CFSTR("BSD Minor"), (__bridge const void *)@(minor(dev)));
            NSLog(@"Will try to match I/O registry entires with: %@", classesToMatch);
        } else {
            NSLog(@"Failed to create I/O registery matcher for IOMedias (logical volumes).");
            return NO;
        }

        // IOServiceGetMatchingServices() CFReleases classesToMatch (even if it returns an error).
        if (KERN_SUCCESS != IOServiceGetMatchingServices(kIOMasterPortDefault, classesToMatch, &entryIterator))
        {
            NSLog(@"Can't iterate services");
            return NO;
        }
    }

    BOOL isSolidState = NO;

    // iterate over all found medias
    io_object_t serviceEntry, parentMedia;
    while ((serviceEntry = IOIteratorNext(entryIterator)))
    {
        io_name_t mediaName;
        if (KERN_SUCCESS != IORegistryEntryGetName(serviceEntry, mediaName)) {
            strlcpy(mediaName, "Unknown", sizeof(mediaName));
        }

        NSLog(@"Found IOMedia \"%s\".", mediaName);

        int maxlevels = 8;
        do
        {
            kern_return_t kernResult = IORegistryEntryGetParentEntry(serviceEntry, kIOServicePlane, &parentMedia);

            if (KERN_SUCCESS != kernResult) {
                NSLog(@"Error while getting parent service entry");
                break;
            }

            IOObjectRelease(serviceEntry);
            serviceEntry = parentMedia;
            if (!parentMedia) break; // finished iterator

            CFTypeRef res = IORegistryEntryCreateCFProperty(serviceEntry, CFSTR(kIOPropertyDeviceCharacteristicsKey), kCFAllocatorDefault, 0);
            if (res)
            {
                NSString *type = [(__bridge NSDictionary*)res objectForKey:(id)CFSTR(kIOPropertyMediumTypeKey)];
                isSolidState = [@"Solid State" isEqualToString:type];
                NSLog(@"Found %sSSD disk %@", (isSolidState ? "" : "non-"), res);
                CFRelease(res);
                if (isSolidState) break;
            }
        }
        while(maxlevels--);

        if (serviceEntry) IOObjectRelease(serviceEntry);
    }
    IOObjectRelease(entryIterator);

    return isSolidState;
}
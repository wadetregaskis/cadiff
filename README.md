cadiff - Content-addressing diff
======

This is essentially 'diff' in binary mode where it identifies files based on their content, not their filename.  It simply lets you compare two sets of data where file names or other metadata may have changed, but all you really care about is whether the actual files - that is, the actual data within each - is duplicated or not.

It's also an interesting exercise in how heavily you can optimise for I/O performance using libdispatch.  The result is not pretty, but effective.

NOTE: this does *not* tell you differences within two files.  This is a different kind of diff.  Though it is on the TODO list to add the ability to tell you how two matches differ (e.g. how their metadata is different; different names, modification dates, etc).

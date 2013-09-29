cadiff - Content-addressing diff
--------------------------------

This is essentially 'diff' in binary mode where it identifies files based on their content, not their filename.  It simply lets you compare two sets of data where file names or other metadata may have changed, but all you really care about is whether the actual files - that is, the actual data within each - is duplicated or not.

It's also an interesting exercise in how heavily you can optimise for I/O performance using libdispatch.  The result is not pretty, but effective.

I (Wade Tregaskis) wrote it because I needed a tool for checking that I'd copied all my photos off my SD cards, possibly many weeks prior and having renamed and moved them all around since then.

NOTE: this does **not** tell you differences within two files.  This is a different kind of diff.  Though something like that is on the TODO list, below.


#### License

Normal 2-clause BSD.  See the top of main.m for details.


#### TODOs

* Add the ability to tell you how two matches differ (e.g. how their metadata is different; different names, modification dates, etc).
* Optimise the indexing (hashing) step by doing multiple passes.  i.e. first hash only the first 4K of each file, then for all the conflicting ones, hash e.g. the next 1 MiB, etc.  This is just a further optimisation of the existing method of hashing only a subset of the file (1 MiB by default) that might be able to further reduce overall I/O while dealing with hash conflicts gracefully.
* Figure out why there appears to be some kind of serialisation occurring, in batches of concurrencyLimiter size.
* Figure out why bandwidth utilisation of the SD card slot, as a percentage, seems inversely proportionate with internal disks'.  i.e. you can get 0 MB/s and 400 MB/s respectively, or 10 MB/s and 200 MB/s, or 20 MB/s and 20 MB/s, but not the obvious 20 MB/s and 400 MB/s.

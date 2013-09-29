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
* Optimise the indexing (hashing) step by doing multiple passes.  i.e. first hash only the first 4K of each file, then for all the conflicting ones, hash e.g. the next 1 MiB, etc.  Or maybe just hash the first 1 MiB only and go straight to the full comparison after that.  Whatever.  Either way this'll dramatically improve the ability to use this for needle-in-a-haystack search-style diffs (e.g. "here's my SD card, here's my entire Aperture library, have at it").
* Figure out why there appears to be some kind of serialisation occurring, in batches of concurrencyLimiter size.
* Figure out why bandwidth utilisation of the SD card slot, as a percentage, seems inversely proportionate with internal disks'.  i.e. you can get 0 MB/s and 400 MB/s respectively, or 10 MB/s and 200 MB/s, or 20 MB/s and 20 MB/s, but not the obvious 20 MB/s and 400 MB/s.

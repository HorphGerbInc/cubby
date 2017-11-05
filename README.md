# cubby

Cubby is a managed storage system that supports lock free read and write operations. Cubby divides a storage collection into fixed width slots.
Each slot in a collection is composed of a metadata header and the contents, which may be encoded with a variety of text encoding schemes or base-64 encoded binary. An in memory LRU cache 
provides fast reads for recently accessed slots, and a garbage collection system automatically frees up cache that exceeds a user-defined cache limit. Slot writes update the in-memory cache
atomically once the write operation is successful, ensuring that any parallel reads still have access to the previous cache. Cache misses during a read operation automatically trigger
a filesystem read.

[![Build Status](http://jeffgabeci.westus2.cloudapp.azure.com:8080/buildStatus/icon?job=Cubby/master)](http://jeffgabeci.westus2.cloudapp.azure.com:8080/job/Cubby/job/master/)

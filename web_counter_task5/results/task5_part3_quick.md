| Test scenarios | Total time | Throughput |
|---|---:|---:|
| In-memory | 79.67 s | 1255 req/s |
| Disk (fsync) | 973.68 s | 103 req/s |
| PostgreSQL (in-place update) | 220.81 s | 453 req/s |
| Hazelcast (IAtomicLong, CP Subsystem) | 77.85 s | 1285 req/s |
| MongoDB (findOneAndUpdate, w=majority) | 696.46 s | 144 req/s |
| Cassandra (counter, RF=3, CL=ONE) | 16.77 s | 596 req/s |
| Cassandra (counter, RF=3, CL=QUORUM) | 14.87 s | 672 req/s |

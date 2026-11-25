| Test scenarios | Total time | Throughput |
|---|---:|---:|
| In-memory | 2.94 s | 3407 req/s |
| Disk (fsync) | 124.31 s | 80 req/s |
| PostgreSQL (in-place update) | 31.11 s | 321 req/s |
| Hazelcast (IAtomicLong, CP Subsystem) | 11.15 s | 897 req/s |
| MongoDB (findOneAndUpdate, w=majority) | 76.42 s | 131 req/s |

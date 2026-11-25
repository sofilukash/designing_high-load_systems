| Test scenarios | Total time | Throughput |
|---|---:|---:|
| In-memory | 2.37 s | 4227 req/s |
| Disk (fsync) | 35.65 s | 281 req/s |
| PostgreSQL (in-place update) | 19.41 s | 515 req/s |
| Hazelcast (IAtomicLong, CP Subsystem) | 7.66 s | 1305 req/s |

| Test scenarios | Total time | Throughput |
|---|---:|---:|
| In-memory | 17.79 s | 5621 req/s |
| Disk (fsync) | 437.83 s | 228 req/s |
| PostgreSQL (in-place update) | 374.83 s | 267 req/s |
| Hazelcast (IAtomicLong, CP Subsystem) | 118.21 s | 846 req/s |

## Tesseract: Online Schema Evolution is (almost) Free for Snapshot Databases

Tesseract is a research snapshot database engine that enables efficient online schema evolution.

Tesseract inherits the shared-everything architecture, synchronization and concurrency control protocol from ERMIA. See our VLDB'21 paper [1] for the use of Coroutine, SIGMOD'16 paper [2] for a description of ERMIA, our VLDBJ paper [3] for details in concurrency control, and our VLDB paper [4] for replication.

\[1\] Yongjun He, Jiacheng Lu and Tianzheng Wang. [CoroBase: Coroutine-Oriented Main-Memory Database Engine](http://www.vldb.org/pvldb/vol14/p431-he.pdf). VLDB 2021.

\[2\] Kangnyeon Kim, Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [ERMIA: Fast Memory-Optimized Database System for Heterogeneous Workloads](https://dl.acm.org/doi/10.1145/2882903.2882905). SIGMOD 2016.

\[3\] Tianzheng Wang, Ryan Johnson, Alan Fekete and Ippokratis Pandis. [Efficiently making (almost) any concurrency control mechanism serializable](https://link.springer.com/article/10.1007/s00778-017-0463-8). The VLDB Journal, Volume 26, Issue 4. 2017.

\[4\] Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [Query Fresh: Log Shipping on Steroids](http://www.vldb.org/pvldb/vol11/p406-wang.pdf). VLDB 2018.

#### Software dependencies
* cmake
* python2
* [clang; libcxx; libcxxabi](https://github.com/llvm/llvm-project)
* libnuma
* libibverbs
* libgflags
* libgoogle-glog
* liburing

Ubuntu
```
apt-get install -y cmake gcc-11 g++-11 clang-10 libc++-8-dev libc++abi-8-dev
apt-get install -y libnuma-dev libibverbs-dev libgflags-dev libgoogle-glog-dev liburing-dev
```

#### Environment configurations
Make sure you have enough huge pages.

* Tesseract uses `mmap` with `MAP_HUGETLB` (available after Linux 2.6.32) to allocate huge pages. Almost all memory allocations come from the space carved out here. Assuming the default huge page size is 2MB, the command below will allocate 2x MB of memory:
```
sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```
This limits the maximum for --node-memory-gb to 10 for a 4-socket machine (see below).

* `mlock` limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your login):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
*Re-login to apply.*

--------
#### Build it
We do not allow building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
$ make -jN
```

Currently the code can compile under Clang 10.0+. E.g., to use Clang 10.0, issue the following `cmake` command instead:
```
$ CC=clang-10.0 CXX=clang++-10.0 cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
```

After `make` there will be six executables under `build`: 

`corobase_DDL_COPY` that runs DDLs with Tesseract approach;

`corobase_DDL_LAZY_COPY` that runs DDLs with lazy approach;

`corobase_DDL_OPT_LAZY_COPY` that runs DDLs with Tesseract-lazy approach;

`corobase_DDL_BLOCK` that runs DDLs with blocking approach;

`corobase_DDL_SI` that runs DDLs with naive SI approach;

`corobase_NO_DDL` that runs with no DDLs;

#### Run it
```
$run.sh \
       [executable] \
       [benchmark] \
       [scale-factor] \
       [num-threads] \
       [duration (seconds)] \
       "[other system-wide runtime options]" \
       "[other benchmark-specific runtime options]"`
```

#### Run example
```
# Add column with Tesseract approach under TPC-CD:
./run.sh ./corobase_DDL_COPY tpcc_org 50 31 10 "-node_memory_gb=30 -cdc_threads=5 -scan_threads=3 -enable_cdc_schema_lock=0 -enable_ddl_keys=0 -pcommit_queue_length=500000 -ddl_total=1 -enable_parallel_scan_cdc=1 -print_interval_ms=1000 -cdc_physical_workers_only=1 -scan_physical_workers_only=1 -client_load_per_core=4500 -latency_stat_interval_ms=25 -enable_large_ddl_begin_timestamp=1" "-d 2 -e 0 -s 1"

# Add column with Tesseract-lazy approach under TPC-CD:
./run.sh ./corobase_DDL_LAZY_COPY tpcc_org 50 31 10 "-node_memory_gb=30 -cdc_threads=5 -scan_threads=3 -enable_cdc_schema_lock=0 -enable_ddl_keys=1 -pcommit_queue_length=500000 -enable_lazy_background=1 -ddl_total=1 -enable_parallel_scan_cdc=1 -print_interval_ms=1000 -cdc_physical_workers_only=1 -scan_physical_workers_only=1 -client_load_per_core=4500 -latency_stat_interval_ms=25 -enable_lazy_on_conflict_do_nothing=1 -late_background_start_ms=0" "-d 2 -e 0 -s 1"

# Add column with lazy approach under TPC-CD:
./run.sh ./corobase_DDL_LAZY_COPY tpcc_org 50 31 10 "-node_memory_gb=30 -cdc_threads=5 -scan_threads=3 -enable_cdc_schema_lock=0 -enable_ddl_keys=1 -pcommit_queue_length=500000 -enable_lazy_background=1 -ddl_total=1 -enable_parallel_scan_cdc=1 -print_interval_ms=1000 -cdc_physical_workers_only=1 -scan_physical_workers_only=1 -client_load_per_core=4500 -latency_stat_interval_ms=25 -enable_lazy_on_conflict_do_nothing=1 -late_background_start_ms=0" "-d 2 -e 0 -s 1"

# Add column with Blocking approach under TPC-CD:
./run.sh ./corobase_DDL_BLOCK tpcc_org 50 31 10 "-node_memory_gb=30 -cdc_threads=5 -scan_threads=3 -enable_cdc_schema_lock=0 -enable_ddl_keys=0 -pcommit_queue_length=500000 -ddl_total=1 -enable_parallel_scan_cdc=1 -print_interval_ms=1000 -cdc_physical_workers_only=1 -scan_physical_workers_only=1 -client_load_per_core=4500 -latency_stat_interval_ms=25" "-d 2 -e 0"
```

#### System-wide runtime options

`-node_memory_gb`: how many GBs of memory to allocate per socket.

`-null_log_device`: Whether to flush log buffer.

`-tmpfs_dir`: location of the log buffer's mmap file. Default: `/tmpfs/`.

`cdc_threads`: number of CDC threads.

`scan_threads`: number of scan threads.

`enable_parallel_scan_cdc`: Whether enable parallel scan and CDC.

#### Benchmark-specific runtime options

`-d 2`: DDL transaction starts after 2 seconds

`-e 0`: DDL workload, see below:
```
TPC-CD:
    0: Add column
    1: Table Split
    2: Preaggregate
    3: Create Index
    4: Table join
    9: Add constraint
    10: Add column and constraint
    
ODDLB:
    0: Add column
    2: Add constraint
    3: Add column and constraint
```

`-w D`: ODDLB - write-heavy workload D.

`-s 1`: TPC-CD - pick a random home warehouse, 0 if not

`-s 100000000`: ODDLB - number of records in the database table.

`-r 10`: 10 queries per transaction.

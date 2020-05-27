H3 library
==========

Installation::

    mkdir -p build && cd build
    cmake ..
    make
    make install
    make test

Run tests::

    make test

For more options, consult the `documentation <../docs/>`_.


RocksDB
=======
Install all dependencies as per https://github.com/facebook/rocksdb/blob/master/INSTALL.md
make shared_lib
make install-shared

Kreon
======
cmake .. -DCMAKE_BUILD_TYPE=Release -DKREON_BUILD_CPACK=TRUE



FlameGraph
==========
Download FlameGraph suite from https://github.com/brendangregg/FlameGraph
Add flamegraph path to $PATH
export PATH=~/git/FlameGraph:$PATH <-- temporary

sudo perf record -F 999 -g -o benchmark_filesystem.raw ./benchmark -d filesystem -s 10M -o 205 -t 1
./generate_flamegraph.sh benchmark_filesystem.raw [path to FlameGraph if not in $PATH]

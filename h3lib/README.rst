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

To build in a Debian container install dependencies first::

    apt-get install cmake libhiredis-dev libglib2.0-dev cppcheck file dpkg-dev

For more options, consult the `documentation <../docs/>`_.

RocksDB
-------
Install all dependencies as per https://github.com/facebook/rocksdb/blob/master/INSTALL.md::

    make shared_lib
    make install-shared

Kreon
-----
``cmake .. -DCMAKE_BUILD_TYPE=Release -DKREON_BUILD_CPACK=TRUE``

Redis Cluster
-------------
Clone the hiredis-vip client from https://github.com/vipshop/hiredis-vip/tree/master.
The driver is build using a patched version of the client (master, commit: f12060498004494a3e1de11f653a8624f3d218c3).
The patch is stored in h3lib root directory, apply it with git, i.e. ``git apply h3lib.patch``.
To run the tests you have to execute script ``run_rediscluster.sh``. Note it will clone/compile the latest redis-server into
directory "rediscluster" created within the working directory. Read issue #118.

Redis
-----
Install the hiredis client library.

FlameGraph
----------
Download FlameGraph suite from https://github.com/brendangregg/FlameGraph. Add flamegraph path to ``$PATH``::

    export PATH=~/git/FlameGraph:$PATH <-- temporary
    sudo perf record -F 999 -g -o benchmark_filesystem.raw ./benchmark -d filesystem -s 10M -o 205 -t 1
    ./generate_flamegraph.sh benchmark_filesystem.raw [path to FlameGraph if not in $PATH]

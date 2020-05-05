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


H3 library
==========

``h3lib`` is an embedded object store in C.

Installation
------------

To compile ``h3lib`` you need CMake v3.10 or latter. H3 uses the ``glib2`` and ``uuid`` libraries, so make sure you have them installed, along with their header files. You also need the ``cppcheck`` utility.

**Note:** For CentOS 7, use the ``cmake3`` package available in EPEL. Replace ``cmake`` in the following commands with ``cmake3``.

**Note:** For macOS, install ``cmake``, ``pkgconfig``, ``glib2``, and ``libuuid`` via MacPorts. ``h3lib`` requires GCC to build, so you also need to install ``gcc9`` and replace the following ``cmake`` command with: ``export CC=<path to gcc>; cmake -DCMAKE_C_COMPILER=$CC <path to h3lib directory>``

For key-value store plugins install the appropriate library dependencies. ``h3lib`` will build and link with any such libraries available:

* For Kreon, use ``cmake .. -DCMAKE_BUILD_TYPE=Release -DKREON_BUILD_CPACK=TRUE``.
* For `RocksDB <https://rocksdb.org>`_, instal all dependencies as per https://github.com/facebook/rocksdb/blob/master/INSTALL.md (``make shared_lib && make install-shared``).
* For `Redis <https://redis.io>`_, install the ``hiredis`` client library.

To enable compression, add the ``-DH3LIB_USE_COMPRESSION`` flag to the ``cmake`` command.

To build and install::

    mkdir -p build && cd build
    cmake ..
    make
    make install

To package (generates RPM file)::

    make package

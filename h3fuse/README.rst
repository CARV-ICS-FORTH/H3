H3 FUSE filesystem
==================

``h3fuse`` is a FUSE filesystem implementation that uses H3 for storage.

Installation
------------

To compile ``h3fuse`` you need CMake v3.10 or latter.

**Note:** For CentOS 7, use the ``cmake3`` package available in EPEL. Replace ``cmake`` in the following commands with ``cmake3``.

Install ``h3lib`` and ``libfuse-dev`` (you can use ``apt-get install libfuse-dev`` in Ubuntu or ``yum install fuse3 fuse3-devel`` in CentOS).

**Note:** For `Kreon <https://github.com/CARV-ICS-FORTH/kreon>`_, use ``cmake -DCMAKE_EXE_LINKER_FLAGS="-lm" ..``.

To build and install::

    mkdir -p build && cd build
    cmake ..
    make
    make install

To package (generates RPM file)::

    make package

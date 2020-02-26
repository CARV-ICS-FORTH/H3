Installation
============

This chapter describes how to compile, package, and install H3 from its source files.

h3lib
-----

To compile ``h3lib`` you need CMake v3.10 or latter. H3 uses the ``glib2`` and ``uuid`` libraries, so make sure you have them installed, along with their header files.

**Note:** For CentOS 7, use the ``cmake3`` package available in EPEL. Replace ``cmake`` in the following commands with ``cmake3``.

**Note:** For macOS, install ``cmake``, ``pkgconfig``, ``glib2``, and ``libuuid`` via MacPorts. ``h3lib`` requires GCC to build, so you also need to install ``gcc9`` and replace the following ``cmake`` command with: ``export CC=<path to gcc>; cmake -DCMAKE_C_COMPILER=<path to gcc> <path to h3lib directory>``

Create and enter build directory::

    mkdir -p build && cd build

Configure ``cmake`` and generate makefiles::

    cmake <path to h3lib directory>

Compile the project::

    make

Install::

    make install

Package (generates RPM file)::

    make package

Run tests::

    make test

pyh3lib
-------

With ``h3lib`` installed, you can compile and install the Python wrapper library ``pyh3lib``, by entering into the respective directory and running::

    python3 setup.py install

If you have installed ``h3lib`` in some non-standard path and ``setup.py install`` fails to find it, run the following and retry (the example assumes ``h3lib`` is installed in ``/usr/local/``)::

	export C_INCLUDE_PATH=/usr/local/include
	export LIBRARY_PATH=/usr/local/lib

To package (creates ``tar.gz`` in ``dist``)::

	python3 setup.py bdist


JH3lib
------
To compile ``JH3lib``, at least Java version 9 is required. The project was developed and built using ``Apache Maven 3.6.0``, so installing the same version is advised. To install ``JH3lib`` enter the respective directory and run::

  mvn install

To package (creates a ``.jar`` in ``target``)::

  (without dependencies) mvn package
  (include dependencies) mvn package assembly:single

To run tests::

  mvn test

**Note:** When packaging/installing ``JH3lib`` the ``-DskipTests`` flag can be used to skip the test phase.

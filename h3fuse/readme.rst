Install libfuse-dev
===================

sudo apt-get update && sudo apt-get install libfuse-dev

`CMake workflow <https://engineering.facile.it/blog/eng/write-filesystem-fuse/>`_

`IBM Example <https://developer.ibm.com/articles/l-fuse/>`_



Alternatively use libfuse v3.9.0
================================
Download `fuse-3.9.0.tar.xz <https://github.com/libfuse/libfuse/releases/download/fuse-3.9.0/fuse-3.9.0.tar.xz>`_
After extracting the libfuse tarball, create a (temporary) build directory and run Meson
::

    mkdir build; cd build
    meson ..
    ninja
    sudo python3 -m pytest test/
    sudo ninja install

Tested with **Meson 0.45.1** and **ninja 1.8.2** on Ubuntu 18.04

In case of link errors (Ubuntu 18.04) you need to adjust the lib instalation path and run ldconfig
::

    sudo ninja uninstall
    meson configure -D libdir=lib
    sudo ninja install
    ldconfig OR ldconfig -n <path>


Mount Umnount paasthrough example
====================================================
./passthrough -d -s -f /tmp/example
./h3fuse -d -s -f /tmp/example -o bucket=b1
fusermount -u /tmp/example



fstest
===================================================
cd to mountpoint
create a bucket i.e. mkdir lala
cd to bucket
prove -rvf path-to-test-suite
prove -rvf path-to-test-suite/tests/chown
prove -rvf /mnt/HDD/git/pjdfstest/tests/chmod/00.t
./pjdfstest mkdir foo 0750


vdbench
===================================================
cd ~/vdbench50407/
./vdbench -vr -f examples/filesys/create_files  <-- run modified test with data-validation enabled





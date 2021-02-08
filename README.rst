H3
===

H3 is an embedded High speed, High volume, and High availability object store, backed by a high-performance key-value store (Kreon, RocksDB, Redis, etc.) or a filesystem. H3 is implemented in the h3lib library, which provides a C-based cloud-friendly API, similar to Amazon's S3. Python and Java wrappers are also available. The H3 FUSE filesystem allows object access using file semantics.

For design details and APIs, consult the `documentation <docs/>`_.

H3 is developed by the `Computer Architecture and VLSI Systems (CARV) <https://www.ics.forth.gr/carv/>`_ lab of the `Institute of Computer Science (ICS) <https://www.ics.forth.gr>`_ at the `Foundation for Research and Technology Hellas (FORTH) <https://www.ics.forth.gr>`_ and is available under the Apache License, Version 2.0. There is also a `list of contributors <CREDITS>`_.

Installation
------------

To install locally, check the ``README`` file in each folder.

A Docker image is also `available <https://hub.docker.com/r/carvicsforth/h3>`_.

Docker images are built with:

* ``docker build -t h3:<version> .`` (release version, binaries only)
* ``docker build --build-arg BUILD_TYPE=Debug --target h3-builder -t h3:<version>-dev .`` (development version, source and binaries with debug symbols included)

Extensions
----------

Use the `h3-benchmark <https://github.com/CARV-ICS-FORTH/h3-benchmark>`_ to measure H3 performance.

The `CSI H3 mount plugin <https://github.com/CARV-ICS-FORTH/csi-h3>`_ (``csi-h3`` for short), allows you to use H3 FUSE for implementing persistent volumes in Kubernetes.

The `h3-support branch <https://github.com/CARV-ICS-FORTH/argo/tree/h3-support>`_ in our Argo fork adds support for using H3 as a workflow artifact repository. `Argo <https://argoproj.github.io>`_ is a workflow engine for Kubernetes.

Acknowledgements
----------------
This project has received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No 825061 (EVOLVE - `website <https://www.evolve-h2020.eu>`_, `CORDIS <https://cordis.europa.eu/project/id/825061>`_).

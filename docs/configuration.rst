Configuration
=============

When initializing H3, through the ``H3_Init()`` function (or similar in Python and Java), you need to specify a configuration file. The configuration file is in ``.ini`` format and should have at least an ``H3`` section to select the storage backend type. Every storage backend groups its respective options in separate sections named after the backend type.

A sample configuration file with default options is shown below::

    [H3]
    # Can be "filesystem", "kreon", or "rocksdb"
    store = filesystem

    [FILESYSTEM]
    root = /tmp/h3/

    [KREON]

    [ROCKSDB]

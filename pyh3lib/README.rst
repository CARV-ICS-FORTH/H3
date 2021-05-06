H3 Python wrapper and tests
===========================

``pyh3lib`` includes a Python interface for ``h3lib`` (``H3`` class) and tests for H3.

Installation
------------

With ``h3lib`` and the ``python3-devel`` package installed (for CentOS), compile and install ``pyh3lib``, by running::

    python3 setup.py install

Or, to do it in a virtual environment::

    python3 -m venv venv
    source venv/bin/activate
    python3 setup.py install

If you have installed ``h3lib`` in some non-standard path and ``setup.py install`` fails to find it, run the following and retry (the example assumes ``h3lib`` is installed in ``/usr/local/``)::

    export C_INCLUDE_PATH=/usr/local/include
    export LIBRARY_PATH=/usr/local/lib

To package, install ``python3-wheel`` (for CentOS) and run::

    python3 setup.py bdist_wheel

This will create a ``.whl`` file in ``dist``, which you can ``pip3 install``.

To run tests::

    mkdir /tmp/h3
    pytest -v -s --storage "file:///tmp/h3" tests

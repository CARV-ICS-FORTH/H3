H3 Python wrapper and tests
===========================

Installation::

    python3 -m venv venv
    source venv/bin/activate
    ./setup.py install

Run tests::

    mkdir /tmp/h3
    pytest -v -s --storage "file:///tmp/h3" tests

For more options, consult the `documentation <../docs/>`_.

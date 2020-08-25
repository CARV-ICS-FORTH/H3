H3 documentation
================

This includes design details and APIs.

Installation
------------

Install dependencies in a virtual environment::

    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt

Install Python library::

    cd ../pyh3lib && ./setup.py install

Compile docs::

    make html

Then open ``_build/html/index.html`` in a browser.

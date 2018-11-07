#######
fresnel
#######
|python badge| |d.py badge|

****************************
fresnel bot for Lighthouse 9
****************************


Installation
============
The following installation methods were tested and verified on
a Debian 10 "buster" system.

Dependencies
------------
- `Python 3.6`_
- PostgreSQL_

Installation requires a git client and the pipenv_ packaging tool.

.. code-block:: console

    $ git clone https://github.com/AkiraSama/fresnel.git
    $ cd fresnel
    $ pipenv sync
    $ pipenv run python -m fresnel --help

To update your fresnel installation:

.. code-block:: console

    $ git pull
    $ pipenv sync


.. Resource Hyperlinks

.. _d.py rewrite: https://github.com/Rapptz/discord.py/tree/rewrite/
.. _Python 3.6: https://www.python.org/downloads/release/python-367/
.. _PostgreSQL: https://www.postgresql.org/
.. _pipenv: https://pipenv.readthedocs.io/en/latest/install/#installing-pipenv


.. |python badge| image:: https://img.shields.io/badge/python-3.6-blue.svg
   :target: `Python 3.6`_
.. |d.py badge| image:: https://img.shields.io/badge/discord.py-rewrite-blue.svg
   :target: `d.py rewrite`_

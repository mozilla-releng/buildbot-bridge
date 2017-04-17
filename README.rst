========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |requires|
        | |coveralls|
    * - package
      - | |version| |downloads| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|

.. |docs| image:: https://readthedocs.org/projects/buildbot-bridge/badge/?style=flat
    :target: https://readthedocs.org/projects/buildbot-bridge
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/mozilla-releng/buildbot-bridge.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/mozilla-releng/buildbot-bridge

.. |requires| image:: https://requires.io/github/mozilla-releng/buildbot-bridge/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/mozilla-releng/buildbot-bridge/requirements/?branch=master

.. |coveralls| image:: https://coveralls.io/repos/mozilla-releng/buildbot-bridge/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/mozilla-releng/buildbot-bridge

.. |version| image:: https://img.shields.io/pypi/v/bbb.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/bbb

.. |commits-since| image:: https://img.shields.io/github/commits-since/mozilla-releng/buildbot-bridge/v2.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/mozilla-releng/buildbot-bridge/compare/v2.0.0...master

.. |downloads| image:: https://img.shields.io/pypi/dm/bbb.svg
    :alt: PyPI Package monthly downloads
    :target: https://pypi.python.org/pypi/bbb

.. |wheel| image:: https://img.shields.io/pypi/wheel/bbb.svg
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/bbb

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/bbb.svg
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/bbb

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/bbb.svg
    :alt: Supported implementations
    :target: https://pypi.python.org/pypi/bbb


.. end-badges

buildbot to taskcluster bridge

* Free software: BSD license

Installation
============

::

    pip install bbb

Documentation
=============

https://buildbot-bridge.readthedocs.io/

Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox

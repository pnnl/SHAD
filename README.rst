SHAD
****

.. image:: https://travis-ci.org/pnnl/SHAD.svg?branch=master
    :target: https://travis-ci.org/pnnl/SHAD
.. image:: https://api.codacy.com/project/badge/Grade/d353a0bb182a47da80e5711c4e39ca0c
    :target: https://www.codacy.com/app/mminutoli/SHAD?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=pnnl/SHAD&amp;utm_campaign=Badge_Grade
.. image:: https://img.shields.io/badge/license-APACHE2-blue.svg
    :target: https://raw.githubusercontent.com/pnnl/SHAD/master/LICENSE.md
.. image:: https://img.shields.io/github/tag/pnnl/SHAD.svg
    :target: http://github.com/pnnl/SHAD/releases
.. image:: https://img.shields.io/github/issues/pnnl/SHAD.svg
    :target: http://github.com/pnnl/SHAD/issues

.. image:: https://github.com/pnnl/SHAD/raw/master/docs/shad_logo_500.jpeg
    :target: https://pnnl.github.io/SHAD
    :align: center


SHAD is the Scalable High-Performance Algorithms and Data-structures C++
library. SHAD is designed as a software stack, composed of three main layers:

- Abstract Runtime Interface: SHAD adopts a shared-memory, task-based,
  programming model, whose main tasking primitives are definide in its runtime
  abstraction layer; this component represents an interface to underlying
  runtime systems, which implement tasking and threading; for portability,
  SHAD can interface with multiple `Runtime Systems`_.
    
- General Purpose Data-structures: SHAD data-structures offer a shared-memory
  abstraction, and provide APIs for parallel access and update; data-structures
  include arrays, vectors, maps and sets.

- Extensions: SHAD extensions are custom libraries built using the underlying
  SHAD components, and/or other extensions; SHAD currently include graph
  data-structures and algorithms.
  
SHAD is written in C++, and requires compiler support for (at least) C++ 11.  To
enable all of the SHAD's features, please review its `Install Dependencies`_ and
`Runtime Systems`_ requirements.

How to cite SHAD
================

In publications SHAD can be cited as [SHAD]_.

.. [SHAD] V. G. Castellana and M. Minutoli, "SHAD: The Scalable High-Performance
          Algorithms and Data-Structures Library," 18th IEEE/ACM International 
          Symposium on Cluster, Cloud and Grid Computing (CCGRID), Washington,
          DC, USA, 2018.

.. image:: https://img.shields.io/badge/DOI-https%3A%2F%2Fdoi.org%2F10.1109%2FCCGRID.2018.00071-blue.svg
    :target: https://doi.org/10.1109/CCGRID.2018.00071
.. image:: https://img.shields.io/badge/BibTex-view-blue.svg
    :target: https://dblp.org/rec/bibtex/conf/ccgrid/CastellanaM18
.. image:: https://img.shields.io/badge/BibTex-download-blue.svg
    :target: https://dblp.org/rec/bib2/conf/ccgrid/CastellanaM18.bib
.. image:: https://img.shields.io/badge/RIS-download-blue.svg
    :target: https://dblp.org/rec/ris/conf/ccgrid/CastellanaM18.ris


Quickstart with Docker
======================

.. code-block:: shell
    
    $ git clone https://github.com/pnnl/SHAD.git shad
    $ cd shad
    $ docker-compose -f docker/docker-compose.yml pull head worker
    $ docker-compose -f docker/docker-compose.yml up -d scale worker=2
    $ docker exec -u mpi -it dokcer_head_1 /bin/bash
    $ cd $HOME/shad
    $ mkdir build && cd build
    $ cmake .. -DCMAKE_BUILD_TYPE=Release -DSHAD_RUNTIME_SYSTEM=GMT
    $ make


To run the unit test of the array on the docker cluster:

.. code-block:: shell

    $ mpiexec -np 2 -ppn 1 --hosts docker_worker_1,docker_worker_2 \
        test/unit_tests/core/shad_array_test


Build Instructions
==================

Install Dependencies
--------------------

GPerftools
^^^^^^^^^^

GPerftools is an optional dependency.  Of the whole GPerftools framework, SHAD
currently uses only tcmalloc when available.  We have seen significant
performance improvement in using tcmalloc over the standard allocator.
Therefore, we recommend its use.  In the case it is not available through your
package manager, you can follow the following basic instruction to build and
install GPerftools.  Please refer to the project page to have more detailed
information.

.. code-block:: shell

    $ git clone https://github.com/gperftools/gperftools.git
    $ cd gperftools
    $ ./autogen.sh
    $ mkdir build && cd build
    $ ../configure --prefix=$GPERFTOOLSROOT
    $ make && make install


where ``$GPERFTOOLSROOT`` is the directory where you want the library to be
installed.

GTest
^^^^^

The Google Test framework is only required if you want to run the unit tests.
On some system, GTest is not available through the package manager.  In those
cases you can install it following these instructions:

.. code-block:: shell
    
    $ git clone https://github.com/google/googletest.git
    $ cd googletest
    $ mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$GTESTROOT
    $ make && make install

where ``$GTESTROOT`` is the directory where you want the library to be
installed.

Runtime Systems
^^^^^^^^^^^^^^^

To fully exploit its features, SHAD requires a supported runtime system or
threading library to be installed. SHAD currently supports:

- `Global Memory and Threading Runtime System (GMT), <https://github.com/pnnl/gmt>`_
- `Intel Threading Building Blocks (TBB), <https://www.threadingbuildingblocks.org/>`_

If such software is not available on the system, SHAD can be compiled and used
with its default (single-threaded) C++ backend.

GMT
"""

SHAD uses the Global Memory and Threading (GMT) Runtime System as backend for
commodity clusters.  GMT requires a Linux OS, C compiler and MPI. It can be
installed using the following commands:

.. code-block:: shell

    $ git clone https://github.com/pnnl/gmt.git
    $ cd gmt
    $ mkdir build && cd build
    $ cmake .. -DCMAKE_INSTALL_PREFIX=$GMT_ROOT \
        -DCMAKE_BUILD_TYPE=Release
    $ make -j <SOMETHING_REASONABLE> && make install

where ``$GMT_ROOT`` is the directory where you want the library to be installed.

Build SHAD
----------

Before attempting to build SHAD, please take a look at the requirements in
`Install Dependencies`_.  In case gtest is not available, compilation of unit
tests may be disabled setting ``SHAD_ENABLE_UNIT_TEST`` to off.  Currently SHAD
has full support for TBB and GMT `Runtime Systems`_.  Future releases will
provide additional backends. Target runtime systems may be specified via the
``SHAD_RUNTIME_SYSTEM`` option: valid values for this option are ``GMT``,
``TBB``, and, ``CPP_SIMPLE``.

.. code-block:: shell

    $ git clone <url-to-SHAD-repo>  # or untar the SHAD source code.
    $ cd shad
    $ mkdir build && cd build
    $ cmake .. -DCMAKE_INSTALL_PREFIX=$SHADROOT        \
        -DCMAKE_BUILD_TYPE=Release                     \
        -DSHAD_RUNTIME_SYSTEM=<SupportedRuntimeSystem> \
        # if using TBB                                 \
        -DTBB_ROOT=$TBBROOT                            \
        # else if using GMT                            \
        -DGMT_ROOT=$GMTROOT                            \
        # endif                                        \
        -DGTEST_ROOT=$GTESTROOT                        \
        -DGPERFTOOLS_ROOT=$GPERFTOOLSROOT
    $ make -j <SOMETHING_REASONABLE> && make install

If you have multiple compilers (or compiler versions) available on your system,
you may want to indicate a specific one using the
``-DCMAKE_CXX_COMPILER=<compiler>`` option.

Build the Documentation
^^^^^^^^^^^^^^^^^^^^^^^

SHAD's documentation is entirely written using Doxygen_.  You can obtain a copy
of Doxygen through your package manager or following the installation
instructions from their website.  To build SHAD's documentation, you need to:

.. code-block:: shell
    
    $ cd shad/build  # cd into your build directory.
    $ cmake .. -DSHAD_ENABLE_DOXYGEN=1
    $ make doxygen

.. _Doxygen: http://www.doxygen.org

Once the documentation is build, you can open with your favorite web browser the
first page with:

.. code-block:: shell
    
    open docs/doxygen/html/index.html  # From your build directory

SHAD Team
=========

- `Vito Giovanni Castellana <vitogiovanni.castellana@pnnl.gov>`_
- `Marco Minutoli <marco.minutoli@pnnl.gov>`_
- `Maurizio Drocco <maurizio.drocco@pnnl.gov>`_

Disclamer Notice
================

This material was prepared as an account of work sponsored by an agency of the
United States Government.  Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in the development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or any
information, apparatus, product, software, or process disclosed, or represents
that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the United
States Government or any agency thereof, or Battelle Memorial Institute. The
views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.

.. raw:: html

   <div align=center>
   <pre style="align-text:center">
   PACIFIC NORTHWEST NATIONAL LABORATORY
   operated by
   BATTELLE
   for the
   UNITED STATES DEPARTMENT OF ENERGY
   under Contract DE-AC05-76RL01830
   </pre>
   </div>



[![Travis Build Status](https://travis-ci.org/pnnl/SHAD.svg?branch=master)](https://travis-ci.org/pnnl/SHAD)
<p align="center">
  <img src="https://github.com/pnnl/SHAD/blob/update-documentation/docs/shad_logo.jpg" width="500"/>
</p>

SHAD is the Scalable High-Performance Algorithms and Data-structures C++ library. SHAD is designed as a software stack, composed of three main layers:
- Abstract Runtime Interface: SHAD adopts a shared-memory, task-based, programming model, whose main tasking primitives are definide in its runtime abstraction layer; this component represents an interface to underlying runtime systems, which implement tasking and threading; for portability, SHAD can interface with multiple [Runtime Systems](#runtime-systems).
- General Purpose Data-structures: SHAD data-structures offer a shared-memory abstraction, and provide APIs for parallel access and update; data-structures include arrays, vectors, maps and sets.
- Extensions: SHAD extensions are custom libraries built using the underlying SHAD components, and/or other extensions; SHAD currently include graph data-structures and algorithms.  
  
SHAD is written in C++, and requires compiler support for (at least) C++ 11.
To enable all of the SHAD's features, please review its [Install Dependencies](#install-dependencies) and [Runtime Systems](#runtime-systems) requirements.

## Build Instructions

### Install Dependencies

#### GPerftools

GPerftools is an optional dependency.  Of the whole GPerftools framework, SHAD currently uses only tcmalloc when available.  We have seen significant performance improvement in using tcmalloc over the standard allocator.  Therefore, we recommend its use.  In the case it is not available through your package manager, you can follow the following basic instruction to build and install GPerftools.  Please refer to the project page to have more detailed information.

```
git clone https://github.com/gperftools/gperftools.git
cd gperftools
./autogen.sh
mkdir build && cd build
../configure --prefix=$GPERFTOOLSROOT
make && make install
```

where ```$GPERFTOOLSROOT``` is the directory where you want the library to be installed.

#### GTest

The Google Test framework is only required if you want to run the unit tests.  On some system, GTest is not available through the package manager.  In those cases you can install it following these instructions:

```
git clone https://github.com/google/googletest.git
cd googletest
mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$GTESTROOT
make && make install
```

where ```$GTESTROOT``` is the directory where you want the library to be installed.

### Runtime Systems
To fully exploit its features, SHAD requires a supported runtime system or threading library to be installed. SHAD currently supports:
- Global Memory and Threading Runtime System (GMT), https://github.com/pnnl/gmt
- Intel Threading Building Blocks (TBB), https://www.threadingbuildingblocks.org/  
If such software is not available on the system, SHAD can be compiled and used with its default (single-threaded) C++ backend.

#### GMT
SHAD uses the Global Memory and Threading (GMT) Runtime System as backend for commodity clusters.
GMT requires a Linux OS, C compiler and MPI. It can be installed using the following commands:

```
git clone https://github.com/pnnl/gmt.git
cd gmt
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$GMT_ROOT \
         -DCMAKE_BUILD_TYPE=Release
make -j <SOMETHING_REASONABLE> && make install
```

#### SPDLOG
SHAD uses spdlog which is very fast, header only and C++ library for logging.
It can be installed using the following commands:

```
git clone https://github.com/gabime/spdlog.git
cd spdlog
mkdir build && cd build
cmake ..
make -j <SOMETHING_REASONABLE>
```

### Build SHAD

Before attempting to build SHAD, please take a look at the requirements in [Install Dependencies](#install-dependencies).
In case gtest is not available, compilation of unit tests may be disabled setting ```SHAD_ENABLE_UNIT_TEST``` to off.
Currently SHAD has full support for TBB and GMT [Runtime Systems](#runtime-systems).  Future releases will provide additional backends. Target runtime systems may be specified via the ```SHAD_RUNTIME_SYSTEM``` option: valid values for this option are ```GMT```, ```TBB```, and, ```CPP_SIMPLE```.

```
git clone <url-to-SHAD-repo>  # or untar the SHAD source code.
cd shad
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$SHADROOT               \
         -DCMAKE_BUILD_TYPE=Release                     \
         -DSHAD_RUNTIME_SYSTEM=<SupportedRuntimeSystem> \
         # if using TBB                                 \
         -DTBB_ROOT=$TBBROOT                            \
         # else if using GMT                            \
         -DGMT_ROOT=$GMTROOT                            \
         # endif                                        \
         -DGTEST_ROOT=$GTESTROOT                        \
         -DGPERFTOOLS_ROOT=$GPERFTOOLSROOT              \
         -DSPDLOG_ROOT=$INCLUDE_DIR_PATH_OF_SPDLOG      \
         -DSHAD_ENABLE_LOGGING = 1
make -j <SOMETHING_REASONABLE> && make install
```
If you have multiple compilers (or compiler versions) available on your system, you may want to indicate a specific one using the ```-DCMAKE_CXX_COMPILER=<compiler>``` option.

### Build the Documentation

SHAD's documentation is entirely written using [Doxygen](http://www.doxygen.org).  You can obtain a copy of Doxygen through your package manager or following the installation instructions from their website.  To build SHAD's documentation, you need to:
```
cd shad/build  # cd into your build directory.
cmake .. -DSHAD_ENABLE_DOXYGEN=1
make doxygen
```

Once the documentation is build, you can open with your favorite web browser the first page with:
```
open docs/doxygen/html/index.html  # From your build directory
```
## Authors
Vito Giovanni Castellana <vitogiovanni.castellana@pnnl.gov>  
Marco Minutoli <marco.minutoli@pnnl.gov>

## Disclamer Notice
This material was prepared as an account of work sponsored by an agency of the United States Government.  Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any of their employees, nor any jurisdiction or organization that has cooperated in the development of these materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process disclosed, or represents that its use would not infringe privately owned rights.

Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer, or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed herein do not necessarily state or reflect those of the United States Government or any agency thereof.

                     PACIFIC NORTHWEST NATIONAL LABORATORY
                                  operated by
                                    BATTELLE
                                    for the
                       UNITED STATES DEPARTMENT OF ENERGY
                        under Contract DE-AC05-76RL01830


[![Travis Build Status](https://travis-ci.org/pnnl/SHAD.svg?branch=master)](https://travis-ci.org/pnnl/SHAD)

# SHAD

## Build Instructions

### Install Dependencies

#### GPerftools

GPerftools is an optional dependency.  Of the whole GPerftools framework, SHAD uses only tcmalloc when available.  We have seen significant performance improvement in using tcmalloc over the standard allocator.  Therefore, we recommend its use.  In the case it is not available through your package manager, you can follow the following basic instruction to build and install GPerftools.  Please refer to the project page to have more detailed information.

```
git clone https://github.com/gperftools/gperftools.git
cd gperftools
./autogen.sh
mkdir build && cd build
../configure --prefix=$GPERFTOOLSROOT
make && make install
```

where $GPERFTOOLSROOT is the directory where you want the library to be installed.

#### GTest

The Google Test framework is only required if you want to run the unit tests.  On some system, GTest is not available through the package manager.  In those cases you can install it following these instructions:

```
git clone https://github.com/google/googletest.git
cd googletest
mkdir build && cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$GTESTROOT
make && make install
```

where $GTESTROOT is the directory where you want the library to be installed.

#### Google Benchmark

The Google Benchmark framework is only required if you want to run the performance benchmarks.  You can install it following these instructions:

```
git clone https://github.com/google/benchmark.git
cd benchmark
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=RELEASE
make
sudo make install
```

### Build SHAD

Before attempting to build SHAD, please take a look at the requirements in [Install Dependencies](#install-dependencies).  In this first release, SHAD has full support only for the TBB run-time system.  Future releases will provide additional backends.

```
git clone <url-to-SHAD-repo>  # or untar the SHAD source code.
cd shad
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=$SHADROOT \
         -DCMAKE_BUILD_TYPE=Release       \
         -DSHAD_RUNTIME_SYSTEM=TBB        \
         -DTBB_ROOT=$TBBROOT              \
         -DGTEST_ROOT=$GTESTROOT          \
         -DGPERFTOOLS_ROOT=$GPERFTOOLSROOT
make -j <SOMETHING_REASONABLE> && make install
```

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

## Acknowledgments
This work was supported in part by the High Performance Data Analytics (HPDA) Program at Pacific Northwest National Laboratory.

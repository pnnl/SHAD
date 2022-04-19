//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2018 Battelle Memorial Institute
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//
//===----------------------------------------------------------------------===//

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_AVAILABLE_MAPPINGS_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_AVAILABLE_MAPPINGS_H_

#if defined HAVE_CPP_SIMPLE
#include "shad/runtime/mappings/cpp_simple/cpp_simple_asynchronous_interface.h"
#include "shad/runtime/mappings/cpp_simple/cpp_simple_synchronous_interface.h"
#elif defined HAVE_TBB
#include "shad/runtime/mappings/tbb/tbb_asynchronous_interface.h"
#include "shad/runtime/mappings/tbb/tbb_synchronous_interface.h"
#elif defined HAVE_GMT
#include "shad/runtime/mappings/gmt/gmt_asynchronous_interface.h"
#include "shad/runtime/mappings/gmt/gmt_synchronous_interface.h"
#elif defined HAVE_HPX
#include "shad/runtime/mappings/hpx/hpx_asynchronous_interface.h"
#include "shad/runtime/mappings/hpx/hpx_synchronous_interface.h"
#else
#error Unsupported Runtime System
#endif
#include "shad/runtime/mappings/available_traits_mappings.h"
#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_AVAILABLE_MAPPINGS_H_

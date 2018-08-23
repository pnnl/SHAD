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

#ifndef INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_TRAITS_MAPPING_H_
#define INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_TRAITS_MAPPING_H_

#include <cstdint>
#include <limits>
#include <mutex>
#include <string>

#include "gmt/gmt.h"
#include "shad/runtime/mapping_traits.h"

namespace shad {
    
    namespace rt {
        namespace impl {
            
            struct gmt_tag {};
            
            template <>
            struct HandleTrait<gmt_tag> {
                using HandleTy = gmt_handle_t;
                using ParameterTy = gmt_handle_t &;
                using ConstParameterTy = const gmt_handle_t &;
                
                static void Init(ParameterTy H, HandleTy V) { H = V; }
                
                static constexpr HandleTy NullValue() { return GMT_HANDLE_NULL; }
                
                static bool Equal(ConstParameterTy lhs, ConstParameterTy rhs) {
                    return lhs == rhs;
                }
                
                static std::string toString(ConstParameterTy H) { return std::to_string(H); }
                
                static uint64_t toUnsignedInt(ConstParameterTy H) { return H; }
                
                static HandleTy CreateNewHandle() { return gmt_get_handle(); }
                
                static void WaitFor(ParameterTy &H) {
                    if (H == NullValue())
                        std::cout << "WARNING: Called wait on a NULL handle" << std::endl;
                    gmt_wait_handle(H);
                    H = NullValue();
                }
            };
            
            template <>
            struct LockTrait<gmt_tag> {
                using LockTy = std::mutex;
                
                static void lock(LockTy &L) {
                    while (!L.try_lock()) gmt_yield();
                }
                
                static void unlock(LockTy &L) { L.unlock(); }
            };
            
            template <>
            struct RuntimeInternalsTrait<gmt_tag> {
                static void Initialize(int argc, char *argv[]) {}
                
                static void Finalize() {}
                
                static size_t Concurrency() { return gmt_num_workers(); }
                static void Yield() { gmt_yield(); }
                
                static uint32_t ThisLocality() { return gmt_node_id(); }
                static uint32_t NullLocality() { return -1; }
                static uint32_t NumLocalities() { return gmt_num_nodes(); }
            };
            
        }  // namespace impl
        
        using TargetSystemTag = impl::gmt_tag;
        
    }  // namespace rt
}  // namespace shad

#endif  // INCLUDE_SHAD_RUNTIME_MAPPINGS_GMT_GMT_TRAITS_MAPPING_H_


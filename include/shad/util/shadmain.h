//===------------------------------------------------------------*- C++ -*-===//
//
//                                     SHAD
//
//      The Scalable High-performance Algorithms and Data Structure Library
//
//===----------------------------------------------------------------------===//
//
// Copyright 2017 Pacific Northwest National Laboratory
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


#ifndef INCLUDE_SHAD_UTIL_SHADMAIN_H_
#define INCLUDE_SHAD_UTIL_SHADMAIN_H_

/// @namespace shad
namespace shad {

/// @brief The entrypoint of a program using SHAD.
///
/// The main within the shad namespace is called after the rest of the SHAD
/// stack is initialized.
int main(int argc, char * argv[]);

}  // namespace shad

#endif  // INCLUDE_SHAD_UTIL_SHADMAIN_H_

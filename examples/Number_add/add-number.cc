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

/*
 Developer      Date            Description
 ================================================================================
 Methun K       07/11/2018      Simple implementation of adding huge amount of numbers to compare the runtime among traditional forloop, shad sync ForEach and AsyncForEach method.
 Methun K       07/26/2018      Replace the console print to log write for testing
 --------------------------------------------------------------------------------
 */

#include <atomic>
#include <random>

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"
#include "shad/util/measure.h"
 #include "shad/util/slog.h"

// Local sum container
std::atomic<size_t> bigSum(0);
std::atomic<size_t> offset(0);

void getRand(size_t index, int& rVal, size_t& range){
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0,range);
    
    rVal = (offset++) + distribution(generator);
    
    offset = offset % 999999;
}

void getAsyncRand(shad::rt::Handle& handle, size_t index, int& rVal, size_t& range){
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0,2*range);
    
    rVal = (offset++) + distribution(generator);
    
    offset = offset % 999989;
}

void accumulateForEach(size_t pos, int& val){
    bigSum += val;
}

void accumulateAsyncForEach(shad::rt::Handle& handle, size_t pos, int& val){
    bigSum += val;
}

template<typename T>
void printArray(T &array, const size_t &size){
    for(size_t i=0;i<size;i++){
        std::cout<<array->At(i)<<",";
    }
    std::cout<<"\n";
}

void syncLoadFor(const size_t& arraySize){
    int a(0);
    auto myarray = shad::Array<int>::Create(arraySize,a);
    
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0,99999);
    
    #if defined HAVE_LOGGING
    auto t1 = shad_clock::now();
    #endif
    
    for(size_t i=0; i<arraySize;i++){
        int rVal = distribution(generator);
        myarray->InsertAt(i, rVal);
    }
    
    #if defined HAVE_LOGGING
    auto t2 = shad_clock::now();
    std::chrono::duration<double> diff = t2-t1;
    auto log_handler = shad::slog::ShadLog::Instance();
    log_handler->printlf("For:Load", diff.count(), nullptr, shad::rt::thisLocality(), shad::rt::thisLocality(), sizeof(size_t), sizeof(std::atomic<size_t>), arraySize);
    
    t1 = shad_clock::now();
    #endif
    
    bigSum = 0;
    for(size_t i=0; i<arraySize;i++){
        bigSum += myarray->At(i);
    }
    
    #if defined HAVE_LOGGING
    t2 = shad_clock::now();
    diff = t2-t1;
    log_handler->printlf("For:Sum", diff.count(), nullptr, shad::rt::thisLocality(), shad::rt::thisLocality(), sizeof(size_t), sizeof(std::atomic<size_t>), arraySize);
    #endif
    
    std::cout<<"Sum: "<<bigSum<<std::endl;
}

void syncLoadForEach(const size_t& arraySize){
    int a(0);
    auto myarray = shad::Array<int>::Create(arraySize,a);
    size_t r = 919199;
    
    myarray->ForEach(getRand, r);
    
    bigSum = 0;
    myarray->ForEach(accumulateForEach);
    
    std::cout<<"Sum: "<<bigSum<<std::endl;
}

void asyncLoadForEach(const size_t& arraySize){
    int a(0);
    auto myarray = shad::Array<int>::Create(arraySize,a);
    size_t r = 898989;
    shad::rt::Handle handle;
    
    myarray->AsyncForEach(handle, getAsyncRand, r);
    shad::rt::waitForCompletion(handle);
    
    //printArray(myarray, arraySize);
    
    bigSum = 0;
    myarray->AsyncForEach(handle, accumulateAsyncForEach);
    shad::rt::waitForCompletion(handle);
    
    std::cout<<"Sum: "<<bigSum<<std::endl;
}

void sumNumbers(size_t arraySize){
    std::cout<<"Array Size:"<<arraySize<<std::endl;
    
    syncLoadFor(arraySize);
    syncLoadForEach(arraySize);
    asyncLoadForEach(arraySize);
}

namespace shad {
int main(int argc, char**argv) {
    std::cout<<"Running...\n";
    size_t arraySize = 0;
    if(argc<2) arraySize = 9999999;
    else arraySize = std::stol(argv[1]);
    std::cout<<argc<<", "<<argv[1]<<std::endl;
    sumNumbers(arraySize);
    
    return 0;
}
}

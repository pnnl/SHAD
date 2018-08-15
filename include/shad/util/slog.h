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
 Methun K       07/24/2018      Creating class under shad namespace that encapsulate logging functionalities
 Methun K       07/26/2018      Create structure for logging items, Create custom loggin message
 Methun K       07/27/2018      Change the logging message format
 Methun K       08/01/2018      Create template for locality
 Methun K       08/14/2018      Add macro to control the log writing
 --------------------------------------------------------------------------------
 */

#ifndef INCLUDE_UTIL_SLOG_H_
#define INCLUDE_UTIL_SLOG_H_

#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <ctime>
#include <sstream>
#include <iomanip>

#include "spdlog/spdlog.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/async.h"
#include "spdlog/fmt/ostr.h"

using shad_clock = std::chrono::system_clock;

namespace shad{
    namespace slog{
        
        struct ShadType
        {
            std::string rtTagName; // GMT, TBB, etc
            std::string eventName;
            std::string execTime;
            std::string execTimeUnit;
            rt::Handle *handle;
            uint32_t sloc; // Source locality
            uint32_t dloc; // Destination locality
            size_t inputSizeInByte;
            size_t outputSizeInByte;
            size_t loopCounter;

            template<typename OStream>
            friend OStream &operator<<(OStream &os, const ShadType &c)
            {
                //#ifdef FORMAT_CSV
                //    return os << c.rtTagName << "," << c.currentTimeStemp << "," << rt::thisLocality() << "," << c.eventName << "," << c.execTime;
                //#elif FORMAT_JSON
                return os << "\"TAG\":\"" << c.rtTagName << "\",\"SL\":" << std::to_string(c.sloc) << ",\"EN\":\"" << c.eventName << "\",\"ET\":" << c.execTime << ",\"ETU\":\"" << c.execTimeUnit << "\",\"H\":" << ((c.handle==nullptr)?"null":std::to_string(static_cast<uint64_t>(*c.handle))) << ", \"DL\":" << std::to_string(c.dloc) << ",\"IS\":" << std::to_string(c.inputSizeInByte) << ",\"OS\":" << std::to_string(c.outputSizeInByte) << ",\"LI\":" << std::to_string(c.loopCounter);
                //#endif
            }
        };

        class ShadLog{
        private:
            size_t counter[2]={0,0};
            
            // @brief Get Today's date
            std::string getTodayDate(){
                auto now = shad_clock::now();
                auto in_time_t = shad_clock::to_time_t(now);
                
                std::stringstream ss;
                ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d");
                return ss.str();
            }
            
            // @brief Get current time
            std::string getCurrentTime(){
                auto now = shad_clock::now();
                auto in_time_t = shad_clock::to_time_t(now);
                
                std::stringstream ss;
                ss << std::put_time(std::localtime(&in_time_t), "%X");
                return ss.str();
            }

            // @brief Get current time
            std::string getCurrentDateTime(){
                auto now = shad_clock::now();
                auto in_time_t = shad_clock::to_time_t(now);
                
                std::stringstream ss;
                ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d_%X");
                return ss.str();
            }

            // @brief Compute time and unit and return them separately
            // Input is a time difference
            template<typename T>
            std::vector<std::string> getTime(T t){
                if(t<1){
                    return {std::to_string(t*1000),"ms"};
                }else if(t>=1 && t<60){
                    return {std::to_string(t),"sec"};
                }else if(t>=60 && t<3600){
                    return {std::to_string(t/60),"min"};
                }else if(t>=3600 && t<86400){
                    return {std::to_string(t/3600),"hr"};
                }else{
                    float d = t/86400;

                    if(d>1&&d<30) return {std::to_string(d),"days"};
                    else if(d>=30&&d<365) return {std::to_string(d/30),"mn"};
                    else{
                        return {std::to_string(d/365),"yr"};
                    }
                }
            }
            
            // @brief Terminate logging functionalities
            void shutDownLogging(){
                // flush all *registered* loggers using a worker thread every 3 seconds.
                // note: registered loggers *must* be thread safe for this to work correctly!
                spdlog::flush_every(std::chrono::seconds(3));
                
                // apply some function on all registered loggers
                spdlog::apply_all([&](std::shared_ptr<spdlog::logger> l) {});
                
                // release any threads created by spdlog, and drop all loggers in the registry.
                spdlog::shutdown();
            }

            // @brief Printing logging information in a file, the suffix of the file name is today's date
            //  arg: vector of parameters
            void printLogInFile(const ShadType& msg){
                try{
                    //spdlog::init_thread_pool(8192, 100); // queue with 8k items and 100 backing thread.
                    std::string logger_name = msg.eventName + "_" + std::to_string((++counter[0])%100000000) + "_" + std::to_string((counter[0]>99999998?(++counter[1])%100000000:counter[1]));
                    auto async_file = spdlog::create_async<spdlog::sinks::daily_file_sink_mt>(logger_name, "logs/" + msg.rtTagName + "_" + std::to_string(static_cast<uint32_t>(msg.sloc)) + ".json", 0, 0);
                    
                    async_file->set_pattern("{\"T\":%t, \"P\":%P, \"TS\":\"%Y-%m-%dT%X.%eZ\", %v},");
                    async_file->info("{}", msg);
                    
                    spdlog::drop(logger_name);
                    
                    //shutDownLogging();
                }catch (const spdlog::spdlog_ex& ex){
                    std::cout << "Log initialization failed: " << ex.what() << std::endl;
                }
            }
        public:
            // @brief Default constructor
            ShadLog() = default;
            
            // @brief Creating singleton object
            static ShadLog *Instance() {
                static ShadLog instance;
                return &instance;
            }

            template<typename SLoc, typename DLoc>
            void printlf(std::string eventName, double execTimeInSec, rt::Handle* handle, SLoc sloc, DLoc dloc, size_t inputSizeInByte, size_t outputSizeInByte, size_t loopCounter=1){
                //std::vector<std::string> v = getTime(execTimeInSec);
                std::string tag = "";
                #if defined HAVE_TBB
                    tag = "TBB";
                #elif defined HAVE_GMT
                    tag = "GMT";
                #endif
                
                #if defined HAVE_LOGGING
                    const ShadType param = {tag, eventName, std::to_string(execTimeInSec), "sec", handle, static_cast<uint32_t>(sloc), static_cast<uint32_t>(dloc), inputSizeInByte, outputSizeInByte, loopCounter};
                
                    printLogInFile(param);
                #endif
            }
        };
    }   // namespace slog
}   // namespace shad

#endif

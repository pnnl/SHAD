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

#ifndef INCLUDE_SHAD_DATA_STRUCTURES_BUFFER_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_BUFFER_H_

#include <algorithm>
#include <array>
#include <atomic>
#include <vector>

#include "shad/data_structures/object_identifier.h"
#include "shad/runtime/runtime.h"

namespace shad {

namespace constants {
/// @todo needs to be configurable
/// Size in bytes of the buffer.
static const size_t kBufferNumBytes = 3072;

template <typename T>
constexpr static T const max(T const a, T const b) {
  return a > b ? a : b;
}
template <typename T>
constexpr static T const min(T const a, T const b) {
  return a < b ? a : b;
}
}  // namespace constants

namespace impl {

/// @brief The Buffer utility.
///
/// Buffer used to aggregate data transfers in insertion methods.
/// It is associated to a DataStructure instance, through its
/// global object identifier, and to the Locality target of the
/// data transfers.
/// @tparam EntryType type of the entries stored in the buffer.
/// @tparam DataStructure DataStructure using the buffer.
template <typename EntryType, typename DataStructure>
class Buffer {
  template <typename, typename>
  friend class BuffersVector;

 public:
  /// Size of the buffer in terms of number of entries.
  constexpr static size_t kBufferSize =
      constants::max(constants::kBufferNumBytes / sizeof(EntryType), 1lu);
  using EntriesArray = std::array<EntryType, kBufferSize>;

  Buffer(const Buffer& rhs)
      : data_(rhs.data_),
        size_(rhs.size_),
        lock_(),
        tgtLoc_(rhs.tgtLoc_),
        oid_(rhs.oid_) {}

  Buffer()
      : size_(0),
        lock_(),
        tgtLoc_(),
        oid_(ObjectIdentifier<DataStructure>::kNullID) {}

  Buffer(const rt::Locality& loc, const ObjectIdentifier<DataStructure>& oid)
      : size_(0), lock_(), tgtLoc_(loc), oid_(oid) {}

  enum State { INSERTED, FLUSH, WAIT_FOR_FLUSH };

  struct FlushArgs {
    EntriesArray data;
    size_t numEntries;
    ObjectIdentifier<DataStructure> oid;
  };

  void FlushBuffer() {
    if (size_ == 0) return;
    FlushArgs args = {data_, size_, oid_};
    auto InsertBufferLambda = [](const FlushArgs& args) {
      auto dsPtr = DataStructure::GetPtr(args.oid);
      for (size_t i = 0; i < args.numEntries; i++) {
        dsPtr->BufferEntryInsert(args.data[i]);
      }
    };
    rt::executeAt(tgtLoc_, InsertBufferLambda, args);
    size_ = 0;
  }

  void AsyncFlushBuffer(rt::Handle& handle) {
    if (size_ == 0) return;
    FlushArgs args = {data_, size_, oid_};
    auto AsyncInsertLambda = [](rt::Handle&, const FlushArgs& args) {
      auto dsPtr = DataStructure::GetPtr(args.oid);
      for (size_t i = 0; i < args.numEntries; i++) {
        dsPtr->BufferEntryInsert(args.data[i]);
      }
    };
    rt::asyncExecuteAt(handle, tgtLoc_, AsyncInsertLambda, args);
    size_ = 0;
  }

  void Insert(const EntryType entry) {
    lock_.lock();
    data_[size_++] = entry;
    if (size_ == kBufferSize) {
      FlushBuffer();
    }
    lock_.unlock();
  }

  void Insert(const EntryType* entry, const size_t num_entries) {
    if (entry == nullptr) throw std::invalid_argument("elem is null");
    if (num_entries > kBufferSize)
      throw std::invalid_argument("num_entries greater than buffer_size");
    lock_.lock();
    size_t pos = size_;
    size_ += num_entries;
    memcpy(&data_[pos], entry, num_entries * sizeof(EntryType));
    if (size_ == kBufferSize) {
      FlushBuffer();
    }
    lock_.unlock();
  }

  void AsyncInsert(rt::Handle& handle, const EntryType& entry) {
    lock_.lock();
    data_[size_++] = entry;
    if (size_ == kBufferSize) {
      AsyncFlushBuffer(handle);
    }
    lock_.unlock();
  }

  void AsyncInsert(rt::Handle& handle, const EntryType* entry,
                   const size_t num_entries) {
    if (entry == nullptr) throw std::invalid_argument("elem is null");
    if (num_entries > kBufferSize)
      throw std::invalid_argument("num_entries greater than buffer_size");
    lock_.lock();
    size_t pos = size_;
    size_ += num_entries;
    memcpy(&data_[pos], entry, num_entries * sizeof(EntryType));
    if (size_ == kBufferSize) {
      AsyncFlushBuffer(handle);
    }
    lock_.unlock();
  }

 private:
  EntriesArray data_;
  size_t size_;
  rt::Lock lock_;
  ObjectIdentifier<DataStructure> oid_;

 protected:
  rt::Locality tgtLoc_;
  explicit Buffer(const ObjectIdentifier<DataStructure>& oid)
      : size_(0), lock_(), tgtLoc_(), oid_(oid) {}
};

/// Vector of buffers, of size NumLocalities-1,
/// accessed by the remote locality ID
template <typename EntryType, typename DataStructure>
class BuffersVector {
 public:
  using BufferType = Buffer<EntryType, DataStructure>;
  explicit BuffersVector(ObjectIdentifier<DataStructure> oid)
      : buffers_(rt::numLocalities(), BufferType(oid)) {
    for (size_t i = 0; i < (rt::numLocalities()); i++) {
      buffers_[i].tgtLoc_ = rt::Locality(i);
    }
  }

  void Insert(const EntryType& entry, const rt::Locality& tgtLoc) {
    uint32_t tgtId = static_cast<uint32_t>(tgtLoc);
    buffers_[tgtId].Insert(entry);
  }

  void AsyncInsert(rt::Handle& handle, const EntryType& entry,
                   const rt::Locality& tgtLoc) {
    uint32_t tgtId = static_cast<uint32_t>(tgtLoc);
    buffers_.at(tgtId).AsyncInsert(handle, entry);
  }

  void FlushAll() {
    for (auto& buffer : buffers_) {
      buffer.FlushBuffer();
    }
  }

  void AsyncFlushAll(rt::Handle& handle) {
    for (auto& buffer : buffers_) {
      buffer.AsyncFlushBuffer(handle);
    }
  }

 private:
  std::vector<BufferType> buffers_;
};

}  // namespace impl
}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_BUFFER_H_

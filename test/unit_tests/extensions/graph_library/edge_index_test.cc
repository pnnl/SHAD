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

#include <vector>

#include "gtest/gtest.h"

#include "shad/extensions/graph_library/edge_index.h"
#include "shad/runtime/runtime.h"

using EIType = shad::EdgeIndex<uint64_t, int>;

static const size_t kToInsert = 4096;
static const size_t kMaxNLSize = 64;
static const uint64_t kMagicValue = 42;

class EdgeIndexTest : public ::testing::Test {
 public:
  EdgeIndexTest() {}
  void SetUp() {
    expectedNumEdges_ = 0;
    expectedNumEdgesAfterErase_ = 0;
    for (size_t i = 0; i < kToInsert; i++) {
      size_t nlsize = std::max<size_t>(i % kMaxNLSize, 1);
      expectedNumEdges_ += nlsize;
      nlsize = nlsize / 2 + nlsize % 2;
      expectedNumEdgesAfterErase_ += nlsize;
    }
    printf("Expected num edges: %lu, (after erase: %lu)\n", expectedNumEdges_,
           expectedNumEdgesAfterErase_);
  }
  void TearDown() {}
  size_t expectedNumEdges_;
  size_t expectedNumEdgesAfterErase_;
};

TEST_F(EdgeIndexTest, InsertTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::forEachOnAll(
      [](const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->Insert(i, i + j);
        }
      },
      oid, kToInsert);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, InsertEdgeListTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::forEachOnAll(  // shad::rt::thisLocality(),
      [](const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        std::vector<int> list(nsize);
        for (size_t j = 0; j < nsize; j++) {
          list[j] = i + j;
        }
        eiptr->InsertEdgeList(i, list.data(), nsize);
      },
      oid, kToInsert);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertEdgeListTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        std::vector<int> list(nsize);
        for (size_t j = 0; j < nsize; j++) {
          list[j] = i + j;
        }
        eiptr->AsyncInsertEdgeList(handle, i, list.data(), nsize);
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertEraseTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          if ((j % 2) != 0u) {
            eiptr->Erase(static_cast<int>(i), i + j);
          }
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdgesAfterErase_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertAsyncEraseTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          if ((j % 2) != 0u) {
            eiptr->AsyncErase(handle, i, i + j);
          }
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdgesAfterErase_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, BufferedInsertTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::forEachOnAll(
      [](const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->BufferedInsert(i, i + j);
        }
      },
      oid, kToInsert);
  eidxPtr->WaitForBufferedInsert();
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, BufferedAsyncInsertTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->BufferedAsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  eidxPtr->WaitForBufferedInsert();
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertForeachNeighborTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);

  auto callFELambda = [](shad::rt::Handle &, const EIType::ObjectID &oid,
                         size_t i) {
    auto ForEachNLambda = [](const uint64_t &src, const int &dest) {
      size_t nsize = std::max<size_t>(src % kMaxNLSize, 1);
      ASSERT_TRUE(dest >= src && dest < (src + nsize));
    };
    auto eiptr = EIType::GetPtr(oid);
    eiptr->ForEachNeighbor(i, ForEachNLambda);
  };
  asyncForEachOnAll(handle, callFELambda, oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertAsyncForeachNeighborTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);

  auto callFELambda = [](shad::rt::Handle &handle, const EIType::ObjectID &oid,
                         size_t i) {
    auto ForEachNLambda = [](shad::rt::Handle &, const uint64_t &src,
                             const int &dest) {
      size_t nsize = std::max<size_t>(src % kMaxNLSize, 1);
      ASSERT_TRUE(dest >= src && dest < (src + nsize));
    };
    auto eiptr = EIType::GetPtr(oid);
    eiptr->AsyncForEachNeighbor(handle, i, ForEachNLambda);
  };
  asyncForEachOnAll(handle, callFELambda, oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertForeachVertexTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max(i % kMaxNLSize, 1lu);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);

  auto ForEachVertexLambda = [](const uint64_t &src, EIType::ObjectID &oid) {
    auto eiptr = EIType::GetPtr(oid);
    //    safe since edge lists are co-located with keys
    auto localIdx = eiptr->GetLocalIndexPtr();
    auto neigh = localIdx->GetNeighbors(src);
    size_t nsize = std::max<size_t>(src % kMaxNLSize, 1ul);
    ASSERT_EQ(neigh->Size(), nsize);
    for (size_t j = 0; j < nsize; j++) {
      ASSERT_TRUE(neigh->Find(src + j));
    }
  };
  eidxPtr->ForEachVertex(ForEachVertexLambda, oid);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertAsyncForeachVertexTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max(i % kMaxNLSize, 1lu);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);

  auto ForEachVertexLambda = [](shad::rt::Handle &handle, const uint64_t &src,
                                EIType::ObjectID &oid) {
    auto eiptr = EIType::GetPtr(oid);
    //    safe since edge lists are co-located with keys
    auto localIdx = eiptr->GetLocalIndexPtr();
    auto neigh = localIdx->GetNeighbors(src);
    size_t nsize = std::max<size_t>(src % kMaxNLSize, 1lu);
    ASSERT_EQ(neigh->Size(), nsize);
    for (size_t j = 0; j < nsize; j++) {
      ASSERT_TRUE(neigh->Find(src + j));
    }
  };
  eidxPtr->AsyncForEachVertex(handle, ForEachVertexLambda, oid);
  shad::rt::waitForCompletion(handle);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertForeachEdgeTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  auto ForEachELambda = [](const uint64_t &src, const int &dest) {
    size_t nsize = std::max<size_t>(src % kMaxNLSize, 1);
    ASSERT_TRUE(dest >= src && dest < (src + nsize));
  };
  eidxPtr->ForEachEdge(ForEachELambda);
  EIType::Destroy(oid);
}

TEST_F(EdgeIndexTest, AsyncInsertAsyncForeachEdgeTest) {
  auto eidxPtr = EIType::Create(kToInsert);
  auto oid = eidxPtr->GetGlobalID();
  shad::rt::Handle handle;
  shad::rt::asyncForEachOnAll(
      handle,
      [](shad::rt::Handle &handle, const EIType::ObjectID &oid, size_t i) {
        auto eiptr = EIType::GetPtr(oid);
        size_t nsize = std::max<size_t>(i % kMaxNLSize, 1);
        for (size_t j = 0; j < nsize; j++) {
          eiptr->AsyncInsert(handle, i, i + j);
        }
      },
      oid, kToInsert);
  shad::rt::waitForCompletion(handle);
  ASSERT_EQ(eidxPtr->Size(), kToInsert);
  ASSERT_EQ(eidxPtr->NumEdges(), expectedNumEdges_);
  auto ForEachELambda = [](shad::rt::Handle &, const uint64_t &src,
                           const int &dest) {
    size_t nsize = std::max<size_t>(src % kMaxNLSize, 1);
    ASSERT_TRUE(dest >= src && dest < (src + nsize));
  };
  eidxPtr->AsyncForEachEdge(handle, ForEachELambda);
  shad::rt::waitForCompletion(handle);
  EIType::Destroy(oid);
}

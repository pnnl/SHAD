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

#include <algorithm>
#include <vector>

#include "gtest/gtest.h"

#include "shad/data_structures/array.h"
#include "shad/runtime/runtime.h"

constexpr static size_t kArraySize = 10001;
static size_t kInitValue = 42;

class ArrayTest : public ::testing::Test {
 public:
  ArrayTest() { inputData_ = std::vector<size_t>(kArraySize, 42); }

  void SetUp() {
    for (size_t i = 0; i < kArraySize; i++) {
      inputData_[i] = i + 1;
    }
  }

  void TearDown() {}
  std::vector<size_t> inputData_;
};

TEST_F(ArrayTest, SyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->InsertAt(i, i + 1);
  }
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, RangedSyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);

  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  edsPtr->InsertAt(0, inputData_.data(), kArraySize);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }

  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }

  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, RangedAsyncInsertAndAsyncGet) {
  std::vector<size_t> values(kArraySize);

  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  edsPtr->AsyncInsertAt(handle, 0, inputData_.data(), kArraySize);
  shad::rt::waitForCompletion(handle);
  ;

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, BufferedSyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->BufferedInsertAt(i, i + 1);
  }
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, BufferedAsyncInsertAndSyncGet) {
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->BufferedAsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->WaitForBufferedInsert();
  for (size_t i = 0; i < kArraySize; i++) {
    size_t value = edsPtr->At(i);
    ASSERT_EQ(value, i + 1);
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

static void applyFun(size_t i, size_t &elem, size_t &incr) {
  ASSERT_EQ(incr, kInitValue);
  ASSERT_EQ(elem, i + 1);
  elem += kInitValue;
}

static void applyFunNoArgs(size_t i, size_t &elem) {
  ASSERT_EQ(elem, i + kInitValue + 1);
  elem += kInitValue;
}

static void applyFunTwoArgs(size_t /*i*/, size_t & /*elem*/, size_t &arg1,
                            size_t &arg2) {
  ASSERT_EQ(arg1, kInitValue);
  ASSERT_EQ(arg2, kInitValue + 2);
}

TEST_F(ArrayTest, AsyncInsertSyncApplyAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFun, kInitValue);
  }
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFunNoArgs);
  }
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->Apply(i, applyFunTwoArgs, arg1, arg2);
  }
  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle2, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle2);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

static void asyncApplyFun(shad::rt::Handle & /*unused*/, size_t i, size_t &elem,
                          size_t &incr) {
  ASSERT_EQ(incr, kInitValue);
  ASSERT_EQ(elem, i + 1);
  elem += kInitValue;
}

static void asyncApplyFunNoArgs(shad::rt::Handle & /*unused*/, size_t i,
                                size_t &elem) {
  ASSERT_EQ(elem, i + kInitValue + 1);
  elem += kInitValue;
}

static void asyncApplyFunTwoArgs(shad::rt::Handle & /*unused*/, size_t /*i*/,
                                 size_t & /*elem*/, size_t &arg1,
                                 size_t &arg2) {
  ASSERT_EQ(arg1, kInitValue);
  ASSERT_EQ(arg2, kInitValue + 2);
}

TEST_F(ArrayTest, AsyncInsertAsyncApplyAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);

  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);

  shad::rt::Handle handle2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFun, kInitValue);
  }
  shad::rt::waitForCompletion(handle2);

  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunNoArgs);
  }
  shad::rt::waitForCompletion(handle2);

  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncApply(handle2, i, asyncApplyFunTwoArgs, arg1, arg2);
  }
  shad::rt::waitForCompletion(handle2);
  shad::rt::Handle handle3;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle3, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle3);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertSyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEachInRange(0lu, kArraySize, applyFun, kInitValue);
  edsPtr->ForEachInRange(0lu, kArraySize, applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEachInRange(0lu, kArraySize, applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertSyncForEachAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->ForEach(applyFun, kInitValue);
  edsPtr->ForEach(applyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  edsPtr->ForEach(applyFunTwoArgs, arg1, arg2);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAsyncForEachInRangeAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFun,
                              kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEachInRange(handle, 0lu, kArraySize, asyncApplyFunTwoArgs,
                              arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, AsyncInsertAsyncForEachAndAsyncGet) {
  std::vector<size_t> values(kArraySize);
  auto edsPtr = shad::Array<size_t>::Create(kArraySize, kInitValue);
  shad::rt::Handle handle;
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncInsertAt(handle, i, i + 1);
  }
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFun, kInitValue);
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunNoArgs);
  size_t arg1 = kInitValue;
  size_t arg2 = kInitValue + 2;
  shad::rt::waitForCompletion(handle);
  edsPtr->AsyncForEach(handle, asyncApplyFunTwoArgs, arg1, arg2);
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    edsPtr->AsyncAt(handle, i, &values[i]);
  }
  shad::rt::waitForCompletion(handle);
  for (size_t i = 0; i < kArraySize; i++) {
    ASSERT_EQ(values[i], i + 1 + (2 * kInitValue));
  }
  shad::Array<size_t>::Destroy(edsPtr->GetGlobalID());
}

TEST_F(ArrayTest, CreateNewArray) {
  auto arrayPtr = shad::impl::array<std::size_t, kArraySize>::Create();

  ASSERT_EQ(arrayPtr->size(), kArraySize);
  ASSERT_EQ(arrayPtr->max_size(), kArraySize);
  using value_type =
      typename shad::impl::array<std::size_t, 123456>::value_type;
  ASSERT_TRUE((std::is_same<std::size_t, value_type>::value));

  arrayPtr->Destroy(arrayPtr->GetGlobalID());
}

TEST_F(ArrayTest, FillArray) {
  auto arrayPtr = shad::impl::array<std::size_t, kArraySize>::Create();

  arrayPtr->fill(0xdeadc0d3);

  ASSERT_EQ(arrayPtr->front(), std::size_t(0xdeadc0d3));
  ASSERT_EQ(arrayPtr->back(), std::size_t(0xdeadc0d3));

  for (size_t i = 0; i < arrayPtr->size(); ++i) {
    ASSERT_EQ(arrayPtr->at(i), std::size_t(0xdeadc0d3)) << i;
    arrayPtr->at(i) = i;
  }

  arrayPtr->Destroy(arrayPtr->GetGlobalID());
}

TEST_F(ArrayTest, ArrayIterator) {
  auto arrayPtr = shad::impl::array<std::size_t, kArraySize>::Create();

  std::size_t splitPoint =
      kArraySize / std::max<std::size_t>(shad::rt::numLocalities(), 2) +
      ((kArraySize % std::max<std::size_t>(shad::rt::numLocalities(), 2) ? 0
                                                                         : 1));

  for (size_t i = 0; i < arrayPtr->size(); ++i) {
    arrayPtr->at(i) = i;
  }
  size_t i = 0;
  for (auto v : *arrayPtr) {
    ASSERT_EQ(v, i) << i;
    ++i;
  }

  auto itr = arrayPtr->begin();
  ASSERT_EQ(*itr, std::size_t(0));

  itr += splitPoint - 10;
  ASSERT_EQ(*itr, splitPoint - 10) << static_cast<std::size_t>(*itr);

  itr -= 10;
  ASSERT_EQ(*itr, splitPoint - 20) << static_cast<std::size_t>(*itr);

  itr += 20;
  ASSERT_EQ(*itr, splitPoint) << static_cast<std::size_t>(*itr);

  itr -= splitPoint - 20;
  ASSERT_EQ(*itr, std::size_t(20)) << static_cast<std::size_t>(*itr);
}

TEST_F(ArrayTest, ArrayIteratorTraitTest) {
  using array_type = shad::impl::array<std::size_t, kArraySize>;
  using iterator_traits = std::iterator_traits<typename array_type::iterator>;

  ASSERT_TRUE((std::is_same<typename iterator_traits::difference_type,
                            typename array_type::difference_type>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::value_type,
                            typename array_type::value_type>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::pointer,
                            typename array_type::pointer>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::reference,
                            typename array_type::reference>::value));
  ASSERT_TRUE((std::is_same<typename iterator_traits::iterator_category,
                            std::random_access_iterator_tag>::value));

  auto array = array_type::Create();

  array->fill(1);

  std::size_t j = 0;
  for (auto itr = array->begin(), end = array->end(); itr < end; ++itr) ++j;
  ASSERT_EQ(j, array->size());

  array->at(kArraySize / 2) = 0;
  array->at((kArraySize / 2) + 1) = 2;

  array->at(array->size() - 2) = 0;
  array->at(array->size() - 1) = 2;

  ASSERT_EQ(array->at(kArraySize / 2), *(array->begin() + kArraySize / 2));

  auto subList = shad::impl::array<std::size_t, 2>::Create();
  subList->at(0) = 0;
  subList->at(1) = 2;

  ASSERT_EQ(subList->at(0), std::size_t(0));
  ASSERT_EQ(subList->at(1), std::size_t(2));

  auto max = std::max_element(array->begin(), array->end());
  ASSERT_EQ(*max, std::size_t(2));

  ASSERT_EQ(std::distance(array->begin(), array->end()), array->size());
  ASSERT_EQ(std::distance(subList->begin(), subList->end()), subList->size());

  ASSERT_EQ(std::distance(array->begin(), array->begin() + (kArraySize / 2)),
            kArraySize / 2);
  ASSERT_EQ(std::distance(array->end(), array->end() - (kArraySize / 2)),
            -(kArraySize / 2));

  auto zero = std::find(array->begin(), array->end(), std::size_t(0));
  ASSERT_EQ(*zero, std::size_t(0)) << zero << " " << array->begin();

  auto two = std::find(array->begin(), array->end(), std::size_t(2));
  ASSERT_EQ(*two, std::size_t(2));

  {
    auto two = std::find(subList->begin(), subList->end(), std::size_t(2));
    ASSERT_EQ(*two, std::size_t(2));

    auto zero = std::find(subList->begin(), subList->end(), std::size_t(0));
    ASSERT_EQ(*zero, std::size_t(0));
  }

  auto first = std::search(array->begin(), array->end(), subList->begin(),
                           subList->end());

  ASSERT_EQ(*first, std::size_t(0)) << *first;
  ASSERT_EQ(first, zero);
  ASSERT_EQ(*(first + 1), std::size_t(2));
  ASSERT_EQ((first + 1), two);

  auto last = std::find_end(array->begin(), array->end(), subList->begin(),
                            subList->end());

  ASSERT_EQ(*last, std::size_t(0));
  ASSERT_EQ(*(last + 1), std::size_t(2));
  ASSERT_NE(first, last);

  shad::rt::executeOnAll(
      [](const std::pair<shad::impl::array<std::size_t, 2>::ObjectID,
                         shad::impl::array<std::size_t, kArraySize>::ObjectID>
             &args) {
        auto subListPtr =
            shad::impl::array<std::size_t, 2>::GetPtr(std::get<0>(args));
        auto arrayPtr = shad::impl::array<std::size_t, kArraySize>::GetPtr(
            std::get<1>(args));

        auto res = std::search(arrayPtr->begin(), arrayPtr->end(),
                               subListPtr->begin(), subListPtr->end());

        ASSERT_EQ(arrayPtr->at(kArraySize / 2),
                  *(arrayPtr->begin() + (kArraySize / 2)));
        ASSERT_EQ(res, arrayPtr->begin() + (kArraySize / 2))
            << shad::rt::thisLocality();
        ASSERT_EQ(*res, std::size_t(0)) << shad::rt::thisLocality();
        ASSERT_EQ(res + 1, arrayPtr->begin() + (kArraySize / 2 + 1))
            << shad::rt::thisLocality();
        ASSERT_EQ(*(res + 1), std::size_t(2)) << shad::rt::thisLocality();
      },
      std::make_pair(subList->GetGlobalID(), array->GetGlobalID()));
}

TEST_F(ArrayTest, LocalRanges) {
  using array_type = shad::impl::array<std::size_t, 42>;
  using array_it = typename array_type::iterator;
  using iterator_traits =
         shad::distributed_iterator_traits<typename array_type::iterator>;
  auto array = array_type::Create();
  auto array2 = array_type::Create();

  for (size_t i = 0; i < array->size(); ++i) {
    array->at(i) = i;
    array2->at(i) = i + 1;
  }

  auto first = array->begin();
  auto last = array->end();
  auto first2 = array2->begin();
  auto localities = iterator_traits::localities(first, last);
  auto startingLoc = localities.begin();
  uint32_t numLoc = localities.size();

  for (auto locality = startingLoc, end = localities.end();
       locality != end; ++locality) {
    shad::rt::executeAt(
      locality,
      [](const std::tuple<array_it, array_it, array_it>& args) {
        auto gbegin = std::get<0>(args);
        auto gend = std::get<1>(args);
        auto local_range = iterator_traits::local_range(gbegin, gend);

        auto begin = local_range.begin();
        auto end = local_range.end();

        auto it = iterator_traits::iterator_from_local(gbegin, gend, begin);
        auto dist = std::distance(gbegin, it);

        auto first2 = std::get<2>(args);
        first2 += dist;

        for (;begin != end; ++begin, ++it, ++first2) {
          ASSERT_EQ((*begin+ 1lu), *first2);
        }
      },
      std::make_tuple(first, last, first2));
  }
}

#include <atomic>
#include <algorithm>
#include <iostream>
#include <numeric>
#include <vector>


#include "shad/runtime/runtime.h"

static const size_t n_elements = 100;
struct myelement_t {
  uint8_t first;
  uint32_t second;
  uint64_t third;
};

// each locality has its own
std::vector<myelement_t> remoteData(n_elements, {0, 0, 0});

namespace shad {

int main(int argc, char **argv) {
  myelement_t val{8, 24, 42};
  std::vector<myelement_t> localData(n_elements, val);
  for (auto loc : rt::allLocalities()) {
    std::cout << "Loc: " << loc << std::endl;
    myelement_t* raddress;
    rt::executeAtWithRet(
      loc,
      [](const size_t&, myelement_t** addr) {
        *addr = remoteData.data();
      }, size_t(0), &raddress
    );
    rt::dma(loc, raddress, localData.data(), n_elements);
    std::tuple<size_t, size_t, size_t> acc(0, 0, 0);
    rt::executeAtWithRet(
      loc,
      [](const size_t&, std::tuple<size_t, size_t, size_t>* addr) {
        size_t acc1(0), acc2(0), acc3(0);
        for (auto el : remoteData) {
          acc1 += el.first;
          acc2 += el.second;
          acc3 += el.third;
        }
        std::tuple<size_t, size_t, size_t> acc(acc1, acc2, acc3);
        *addr = acc;
      }, size_t(0), &acc
    );
    std::cout << "(R)Acc1: " << std::get<0>(acc) << ", expected: " << 8*n_elements << std::endl;
    std::cout << "(R)Acc2: " << std::get<1>(acc) << ", expected: " << 24*n_elements << std::endl;
    std::cout << "(R)Acc3: " << std::get<2>(acc) << ", expected: " << 42*n_elements << std::endl;

    localData = std::vector<myelement_t>(n_elements, {0, 0, 0});
    // from remote to local
    rt::dma(localData.data(), loc, raddress, n_elements);
    size_t acc1(0), acc2(0), acc3(0);
    for (auto el : localData) {
      acc1 += el.first;
      acc2 += el.second;
      acc3 += el.third;
    }
    std::cout << "(L)Acc1: " << acc1 << ", expected: " << 8*n_elements << std::endl;
    std::cout << "(L)Acc2: " << acc2 << ", expected: " << 24*n_elements << std::endl;
    std::cout << "(L)Acc3: " << acc3 << ", expected: " << 42*n_elements << std::endl;

    // exercise 2
    // send a task to any locality to update their own data
    // then do a dma to the caller locality (write localData)
    // PLUS: async is better
    for (auto loc2 : rt::allLocalities()) {
        if (loc2 != loc) {
            loc = loc2;
            break;
        }
    }
    rt::Handle h;
    std::tuple<rt::Locality, myelement_t*> argsTuple(rt::thisLocality(), localData.data());
    rt::asyncExecuteAt(h, loc,
      [](rt::Handle& h, const std::tuple<rt::Locality, myelement_t*> &args) {
        myelement_t el({1, 1, 1});
        std::fill(remoteData.begin(), remoteData.end(), el);
        rt::asyncDma(h, std::get<0>(args), std::get<1>(args), remoteData.data(), n_elements);
      },
      argsTuple
    );
    rt::waitForCompletion(h);
    size_t field_cnt = 0;
    for (auto el : localData) {
        field_cnt += el.first;
    }
    std::cout << "\n After we got data back, AGGR(first) = " << field_cnt << std::endl;

  }


  return 0;
}

}  // namespace shad


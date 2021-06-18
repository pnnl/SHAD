#ifndef INCLUDE_SHAD_DATA_STRUCTURES_MY_NEW_DS_H_
#define INCLUDE_SHAD_DATA_STRUCTURES_MY_NEW_DS_H_

#include "shad/data_structures/abstract_data_structure.h"

namespace shad {

/// @brief Wrapper that instantiate one object per Locality in the system.
///
/// This Wrapper triggers the instantiation of one object per Locality in the
/// system.
///
/// @warning Writes are not propagated across the system.
///
/// @tparam T The typen of the objects that will be instantiated.
template <typename T>
class MyNewDS : public AbstractDataStructure<MyNewDS<T>> {
 public:
  using ObjectID = typename AbstractDataStructure<MyNewDS<T>>::ObjectID;
  using SharedPtr =
      typename AbstractDataStructure<MyNewDS<T>>::SharedPtr;

  /// @brief Create method.
  ///
  /// Creates instances of a T object on each locality.
  ///
  /// @tparam Args The list of types needed to build an instance of type T
  ///
  /// @param args The parameter pack to be forwarded to T' constructor.
  /// @return A shared pointer to the local MyNewDS instance.
#ifdef DOXYGEN_IS_RUNNING
  template <typename... Args>
  static SharedPtr Create(Args... args);
#endif

  /// @brief Retieve the Global Identifier.
  ///
  /// @return The global identifier associated with the array instance.
  ObjectID GetGlobalID() const { return oid_; }

  /// @brief Access the local instance.
  ///
  /// @return A pointer to the local instance.
  T* const operator->() { return &localInstance_; }

  /// @brief Assign the an instance of T to the local object.
  ///
  /// @tparam T the type of the allocated.
  /// @param rhs The right-hand side of the assignment.
  MyNewDS<T>& operator=(const T& rhs) {
    localInstance_ = rhs;
    return *this;
  }

  /// @brief Retrieve a copy of the local instance.
  explicit operator T() const { return localInstance_; }

  /// @brief Scatter operation
  void scatter(std::vector<T> &v) {
    uint32_t idx = 0;
    rt::Handle h;
    for (auto loc : shad::rt::allLocalities()) {
      std::pair<ObjectID, T> args(oid_, v[idx]);
      rt::asyncExecuteAt(h, loc,
        [](rt::Handle&, const std::pair<ObjectID, T> &args) {
          auto ptr = MyNewDS<T>::GetPtr(args.first);
          (*ptr) = args.second;
        },  
        args);
      ++idx;
    }
    rt::waitForCompletion(h);
  }


  // Exercise
  // Implement the gather operation
  // Hint1: Very similar to the scatter
  // Hint2: We have seen the needed shad::rt function in ex2_rdma
  std::vector<T> gather() {
    std::vector<T> res(rt::numLocalities());

    return res;
  }

 protected:
  /// @brief Constructor.
  template <typename... Args>
  explicit MyNewDS(ObjectID oid, Args... args)
      : oid_{oid}, localInstance_{args...} {}

 private:
  template <typename>
  friend class AbstractDataStructure;

  ObjectID oid_;
  T localInstance_;
};

}  // namespace shad

#endif  // INCLUDE_SHAD_DATA_STRUCTURES_MY_NEW_DS_H_
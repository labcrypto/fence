#ifndef _IR_NTNAEEM_GATE__SLAVE__BAG_H_
#define _IR_NTNAEEM_GATE__SLAVE__BAG_H_

#include <vector>
#include <mutex>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  template <class M>
  class Bag {
  public:
    Bag() {}
    virtual ~Bag() {}
  public:
    void
    Put(M *item) {
      items_.push_back(item);
    }
    uint32_t
    Size() {
      return items_.size();
    }
    std::vector<M*>
    PopAll() {
      return std::move(items_);
    }
    void
    Purge() {
      for (uint32_t i = 0; i < items_.size(); i++) {
        delete items_[i];
      }
    }
  private:
    std::vector<M*> items_;
  };
}
}
}
}

#endif
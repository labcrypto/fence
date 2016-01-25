#ifndef _IR_NTNAEEM_GATE__MASTER__BAG_H_
#define _IR_NTNAEEM_GATE__MASTER__BAG_H_

#include <vector>
#include <mutex>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
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
  private:
    std::vector<M*> items_;
  };
}
}
}
}

#endif
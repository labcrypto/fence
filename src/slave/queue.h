#ifndef _IR_NTNAEEM_GATE__SLAVE__QUEUE_H_
#define _IR_NTNAEEM_GATE__SLAVE__QUEUE_H_

#include <vector>
#include <map>
#include <string>
#include <mutex>


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  template<class M>
  class Queue {
  public:
    Queue() {}
    virtual ~Queue() {}
  public:
    void
    Enq(M *item) {
      std::lock_guard<std::mutex> guard(lock_);
      if (item == NULL) {
        return;
      }
      items_.push_back(item);
    }
    M*
    Deq() {
      std::lock_guard<std::mutex> guard(lock_);
      if (items_.size() == 0) {
        return NULL;
      }
      M *item = items_[0];
      items_.erase(items_.begin());
      return item;
    }
    uint32_t
    Size() {
      std::lock_guard<std::mutex> guard(lock_);
      return items_.size();
    }
    bool
    HasMore() {
      return Size() > 0;
    }
    void
    Purge() {
      for (uint32_t i = 0; i < items_.size(); i++) {
        delete items_[i];
      }
    }
  private:
    std::mutex lock_;
    std::vector<M*> items_;
  };
}
}
}
}

#endif
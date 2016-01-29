#ifndef _IR_NTNAEEM_GATE__SLAVE__LAEBL_QUEUE_MAP_H_
#define _IR_NTNAEEM_GATE__SLAVE__LAEBL_QUEUE_MAP_H_

#include <vector>
#include <map>
#include <mutex>

#include "queue.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  template<class M>
  class LabelQueueMap {
  public:
    LabelQueueMap() {}
    virtual ~LabelQueueMap() {
      for (uint32_t i = 0; i < queues_.size(); i++) {
        delete queues_[i];
      }
    }
  public:
    void 
    Put(std::string label, M *m) {
      std::lock_guard<std::mutex> guard(lock_);
      if (queuesMap_.find(label) == queuesMap_.end()) {
        Queue<M> *q = new Queue<M>();
        q->Enq(m);
        queuesMap_[label] = q;
        queues_.push_back(q);
      } else {
        queuesMap_[label]->Enq(m);
      }
    }
    uint32_t
    Size() {
      return queuesMap_.size();
    }
    M* 
    Next(std::string label) {
      std::lock_guard<std::mutex> guard(lock_);
      if (queuesMap_[label] == NULL) {
        return NULL;
      }
      return queuesMap_[label]->Deq();
    }
    uint32_t 
    HasMore(std::string label) {
      std::lock_guard<std::mutex> guard(lock_);
      return queuesMap_[label]->HasMore();
    }
  private:
    std::mutex lock_;
    std::map<std::string, Queue<M>*> queuesMap_;
    std::vector<Queue<M>*> queues_;
  };
}
}
}
}

#endif
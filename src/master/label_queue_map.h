#ifndef _IR_NTNAEEM_GATE__MASTER__LAEBL_QUEUE_MAP_H_
#define _IR_NTNAEEM_GATE__MASTER__LABEL_QUEUE_MAP_H_

#include <vector>
#include <map>
#include <mutex>

#include "queue.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  template<class M>
  class LabelQueueMap {
  /* public:
    template<class V>
    class Pair {
    public:
      Pair(std::string label) 
        : label_(label) {
      }
      virtual ~Pair() {}
    public:
      std::string label_;
      std::vector<V*> messages;
    }; */
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
      if (queues_.find(label) != queues_.end()) {
        Queue<M> *q = new Queue<M>();
        q->Enq(m);
        queuesMap_[label] = q;
        // counts_[label] = 1;
        queues_.push_back(q);
      } else {
        queuesMap_[label]->push_back(m);
        // counts_[label]++;
      }
    }
    M* 
    Next(std::string label) {
      std::lock_guard<std::mutex> guard(lock_);
      if (queuesMap_[label] == NULL) {
        return NULL;
      }
      return queuesMap_[label]->Deq();
      /* if (queues_[label]->size() > 0) {
        M* top = queues_[label]->at(0);
        queues_[label]->erase(queues_[label]->begin());
        return top;
      } else {
        return NULL;
      } */
    }
    uint32_t 
    HasMore(std::string label) {
      std::lock_guard<std::mutex> guard(lock_);
      return queuesMap_[label]->HasMore();
    }
    /* void GetMessages(std::vector<M*> &out) {
      std::lock_guard<std::mutex> guard(lock_);
      for (typename std::map<std::string, std::vector<M*>*>::iterator it = queues_.begin();
           it != queues_.end();
           it++) {
        for (uint32_t i = 0; i < it->second->size(); i++) {
          out.push_back(it->second[i].at(i));
        }
      }
    } */
  private:
    // std::map<std::string, uint32_t> counts_;
    std::mutex lock_;
    std::map<std::string, Queue<M>*> queuesMap_;
    std::vector<Queue<M>*> queues_;
  };
}
}
}
}

#endif
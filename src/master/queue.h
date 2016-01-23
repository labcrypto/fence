#ifndef _IR_NTNAEEM_GATE__MAP_H_
#define _IR_NTNAEEM_GATE__MAP_H_

#include <vector>
#include <map>
#include <string>
#include <mutex>


namespace ir {
namespace ntnaeem {
namespace gate {
  template<class M>
  class Queue {
  public:
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
    };
  public:
    Queue() {}
    virtual ~Queue() {}
  public:
    void Put(std::string label, M *m) {
      std::lock_guard<std::mutex> guard(lock_);
      if (map_.find(label) != map_.end()) {
        std::vector<M*> *v = new std::vector<M*>();
        v->push_back(m);
        map_.insert(std::pair<std::string, std::vector<M*>*>(label, v));
      }
      map_.find(label)->second->push_back(m);
    }
    void Get(std::string label, std::vector<M*> &out) {
      std::lock_guard<std::mutex> guard(lock_);
    }
    void GetMessages(std::vector<M*> &out) {
      std::lock_guard<std::mutex> guard(lock_);
      for (typename std::map<std::string, std::vector<M*>*>::iterator it = map_.begin();
           it != map_.end();
           it++) {
        for (uint32_t i = 0; i < it->second->size(); i++) {
          out.push_back(it->second[i].at(i));
        }
      }
    }
  private:
    std::map<std::string, std::vector<M*>*> map_;
    std::mutex lock_;
  };
}
}
}

#endif
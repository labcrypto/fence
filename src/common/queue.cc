#include "queue.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  template<class M>
  void 
  Queue<M>::Put(std::string label, M *m) {
    std::lock_guard<std::mutex> guard(lock_);
    if (map_.find(label) == std::map<std::string, std::vector<M*>*>::nopos) {
      std::vector<M*> v = new std::vector<M*>();
      v->push_back(m);
      map_.put(std::pair<std::string, std::vector<M*>*>(label, v));
    }
    map_.find(label).second->push_back(m);
  }
  template<class M>
  void 
  Queue<M>::Get(std::string label, std::vector<M*> &out) {
    std::lock_guard<std::mutex> guard(lock_);
  }
  template<class M>
  void 
  Queue<M>::GetPairs(std::vector<Pair<M*>*> &out) {
    std::lock_guard<std::mutex> guard(lock_);
  }
}
}
}
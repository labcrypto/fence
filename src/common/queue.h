#ifndef _IR_NTNAEEM_GATE__MAP_H_
#define _IR_NTNAEEM_GATE__MAP_H_

#include <vector>
#include <map>
#include <string>


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
      std::vector<V*> envelopes_;
    };
  public:
    Queue() {}
    virtual ~Queue() {}
  public:
    void Put(std::string label, M *m) {
      if (map_.find(label) == std::nopos) {
        std::vector<M*> v = new std::vector<M*>();
        map_.put(std::pair<std::string, std::vector<M*>*>(label, v));
      }
      map_.find(label).second->push_back(envelope);
    }
    void Get(std::string label, std::vector<M*> &out);
    void GetPairs(std::vector<Pair<M*>*> &out);
  private:
    std::map<std::string, std::vector<M*>*> map_;
  };
}
}
}

#endif
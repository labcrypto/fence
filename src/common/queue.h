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
      std::vector<V*> envelopes_;
    };
  public:
    Queue() {}
    virtual ~Queue() {}
  public:
    void Put(std::string label, M *m);
    void Get(std::string label, std::vector<M*> &out);
    void GetPairs(std::vector<Pair<M*>*> &out);
  private:
    std::map<std::string, std::vector<M*>*> map_;
    std::mutex lock_;
  };
}
}
}

#endif
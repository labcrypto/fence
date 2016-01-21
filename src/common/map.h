#ifndef _IR_NTNAEEM_GATE__MAP_H_
#define _IR_NTNAEEM_GATE__MAP_H_

#include <vector>
#include <map>
#include <string>


namespace ir {
namespace ntnaeem {
namespace gate {
  class Envelope;
  class Map {
  public:
    class Pair {
    public:
      Pair(std::string label) 
        : label_(label) {
      }
      virtual ~Pair() {}
    public:
      std::string label_;
      std::vector< ::ir::ntnaeem::gate::Envelope*> envelopes_;
    };
  public:
    Map() {}
    virtual ~Map() {}
  public:
    void Put(std::string label, ::ir::ntnaeem::gate::Envelope *envelope) {
      if (map_.find(label) == std::nopos) {
        std::vector< ::ir::ntnaeem::gate::Envelope*> v = 
          new std::vector< ::ir::ntnaeem::gate::Envelope*>();
        map_.put(std::pair<std::string, std::vector< ::ir::ntnaeem::gate::Envelope*>*>(label, v));
      }
      map_.find(label).second->push_back(envelope);
    }
    void Get(std::string label, std::vector<Envelope*> &envelopes);
    void GetPairs(std::vector<Pair*> &pairs);
  private:
    std::map<std::string, std::vector< ::ir::ntnaeem::gate::Envelope*>*> map_;
  };
}
}
}

#endif
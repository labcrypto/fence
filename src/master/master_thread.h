#ifndef _IR_NTNAEEM_GATE__MASTER__MASTER_THREAD_H_
#define _IR_NTNAEEM_GATE__MASTER__MASTER_THREAD_H_


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  class MasterThread {
  public:
    static void Start();
    static void ThreadBody();
  };
}
}
}
}
#endif
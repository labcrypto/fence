#ifndef _ORG_LABCRYPTO__FENCE__MASTER__MASTER_THREAD_H_
#define _ORG_LABCRYPTO__FENCE__MASTER__MASTER_THREAD_H_


namespace ir {
namespace ntnaeem {
namespace gate {
namespace master {
  class MasterThread {
  public:
    static void Start();
    static void* ThreadBody(void *);
  };
}
}
}
}
#endif
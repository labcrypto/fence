#ifndef _ORG_LABCRYPTO__FENCE__MASTER__MASTER_THREAD_H_
#define _ORG_LABCRYPTO__FENCE__MASTER__MASTER_THREAD_H_


namespace org {
namespace labcrypto {
namespace fence {
namespace master {
  class MasterThread {
  public:
    static void Start();
    static void* ThreadBody(void *);
  };
} // END NAMESPACE master
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
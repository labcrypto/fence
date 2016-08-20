#ifndef _ORG_LABCRYPTO__FENCE__SLAVE__SLAVE_THREAD_H_
#define _ORG_LABCRYPTO__FENCE__SLAVE__SLAVE_THREAD_H_


namespace org {
namespace labcrypto {
namespace fence {
namespace slave {
  class SlaveThread {
  public:
    static void Start();
    static void* ThreadBody(void *);
  };
} // END NAMESPACE slave
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org
#endif
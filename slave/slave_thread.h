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
}
}
}
}
#endif
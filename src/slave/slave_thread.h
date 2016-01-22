#ifndef _IR_NTNAEEM_GATE__SLAVE__SLAVE_THREAD_H_
#define _IR_NTNAEEM_GATE__SLAVE__SLAVE_THREAD_H_

#include "../common/runtime.h"


namespace ir {
namespace ntnaeem {
namespace gate {
namespace slave {
  class SlaveThread {
  public:
    static void Start();
    static void ThreadBody();
  };
}
}
}
}
#endif
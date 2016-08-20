/*  The MIT License (MIT)
 *
 *  Copyright (c) 2015 LabCrypto Org.
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *  
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *  
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__SUBMITTER_THREAD_H_
#define _ORG_LABCRYPTO__FENCE__CLIENT__SUBMITTER_THREAD_H_

#include <string>
#include <mutex>

#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class SubmitterThread {
  public:
    SubmitterThread (
      std::string fenceHost,
      uint16_t fencePort,
      std::string enqueueLabel,
      std::string workDirPath,
      Runtime *runtime
    ) : fencePort_(fencePort),
        fenceHost_(fenceHost),
        enqueueLabel_(enqueueLabel),
        workDirPath_(workDirPath),
        terminated_(false),
        threadTerminated_(false),
        runtime_(runtime) {
    }
    virtual ~SubmitterThread() {}
  public:
    virtual void Start();
    virtual void Shutdown();
    static void* ThreadBody(void *);
  private:
    uint16_t fencePort_;
    std::string fenceHost_;
    std::string enqueueLabel_;
    std::string workDirPath_;
    bool terminated_;
    bool threadTerminated_;
    std::mutex terminationLock_;
    Runtime *runtime_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
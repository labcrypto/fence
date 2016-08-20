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
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__RUNTIME_H_
#define _ORG_LABCRYPTO__FENCE__CLIENT__RUNTIME_H_

#include <mutex>
#include <deque>
#include <map>
#include <unistd.h>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class Runtime {
  public:
    Runtime() {
      initialized_ = false;
    }
  public:
    void Init(std::string workDirPath, int agrc = 0, char **argv = NULL);
    void Shutdown();
  public:
    static void RegisterWorkDirPath(std::string workDirPath) {
      if (runtimes_.find(workDirPath) == runtimes_.end()) {
        runtimes_[workDirPath] = new Runtime();
      }
    }
    static Runtime* GetRuntime(std::string workDirPath) {
      if (runtimes_.find(workDirPath) == runtimes_.end()) {
        throw std::runtime_error(
          "Work dir path does not exist in the runtime map: '" + 
            workDirPath + "'");
      }
      return runtimes_[workDirPath];
    }
    static void Destroy() {
      for (std::map<std::string, Runtime*>::iterator it = runtimes_.begin();
         it != runtimes_.end();
        ) {
        delete it->second;
        runtimes_.erase(it++);
      }
    }
  public:
    bool initialized_;
    std::mutex messageIdCounterLock_;
    std::mutex mainLock_;

    uint64_t messageIdCounter_;
    std::deque<uint64_t> enqueued_;
    std::map<std::string, std::deque<uint64_t>*> received_;
    std::map<std::string, std::map<uint64_t, uint64_t>*> poppedButNotAcked_;
  private:
    static std::map<std::string, Runtime*> runtimes_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
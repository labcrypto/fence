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
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__MESSAGE_RECEIVER_H_
#define _ORG_LABCRYPTO__FENCE__CLIENT__MESSAGE_RECEIVER_H_

#include <stdint.h>

#include <iomanip>
#include <string>

#include "message.h"
#include "runtime.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class MessageReceiver {
  public:
    MessageReceiver (
      std::string fenceHost,
      uint16_t fencePort,
      std::string popLabel,
      std::string workDirPath
    ) : fencePort_(fencePort),
        fenceHost_(fenceHost),
        popLabel_(popLabel),
        workDirPath_(workDirPath),
        runtime_(NULL) {
    }
    virtual ~MessageReceiver() {}
  public:
    inline uint16_t GetGatePort() const {
      return fencePort_;
    }
    inline std::string GetGateHost() {
      return fenceHost_;
    }
    inline std::string GetPopLabel() {
      return popLabel_;
    }
    inline std::string GetWorkDirPath() {
      return workDirPath_;
    }
  public:
    virtual void 
    Init (
      int agrc = 0, 
      char **argv = NULL
    ) = 0;
    virtual void Shutdown() = 0;
    virtual std::vector<Message*>
    GetMessages () = 0;
    virtual void
    Ack (
      std::vector<uint64_t> ids
    ) = 0;
    virtual void
    Ack (
      uint64_t id
    ) {
      std::vector<uint64_t> ids;
      ids.push_back(id);
      Ack(ids);
    }
  protected:
    uint16_t fencePort_;
    std::string fenceHost_;
    std::string popLabel_;
    std::string workDirPath_;
    Runtime *runtime_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
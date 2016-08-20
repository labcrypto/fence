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
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__DEFAULT_MESSAGE_SUBMITTER_H_
#define _ORG_LABCRYPTO__FENCE__CLIENT__DEFAULT_MESSAGE_SUBMITTER_H_

#include "message_submitter.h"
#include "submitter_thread.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class DefaultMessageSubmitter : public MessageSubmitter {
  public:
    DefaultMessageSubmitter (
      std::string gateHost,
      uint16_t gatePort,
      std::string enqueueLabel,
      std::string workDirPath
    ) : MessageSubmitter (
          gateHost, 
          gatePort, 
          enqueueLabel, 
          workDirPath
        ) {
    }
    virtual ~DefaultMessageSubmitter() {
    }
  public:
    virtual void 
    Init (
      int argc, 
      char **argv
    );
    virtual void Shutdown();
    virtual uint64_t 
    SubmitMessage (
      unsigned char *data, 
      uint32_t length
    );
    virtual uint64_t 
    ReplyMessage (
      uint64_t sourceMessageId,
      unsigned char *data, 
      uint32_t length
    );
  private:
    SubmitterThread *submitterThread_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
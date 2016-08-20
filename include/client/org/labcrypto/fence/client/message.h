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
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__MESSAGE_H_
#define _ORG_LABCRYPTO__FENCE__CLIENT__MESSAGE_H_

#include <stdint.h>

#include <iomanip>
#include <string>

#include "byte_array.h"


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class Message {
  public:
    Message() {
    }
    Message(const Message &other) {
      id_ = other.id_;
      relId_ = other.relId_;
      label_ = other.label_;
      content_ = other.content_;
    }
    Message(Message *other) {
      id_ = other->id_;
      relId_ = other->relId_;
      label_ = other->label_;
      content_ = other->content_;
    }
    virtual ~Message() {}
  public:
    inline uint64_t GetId() const {
      return id_;
    }
    inline void SetId(uint64_t id) {
      id_ = id;
    }
    inline uint64_t GetRelId() const {
      return relId_;
    }
    inline void SetRelId(uint64_t relId) {
      relId_ = relId;
    }
    inline std::string GetLabel() const {
      return label_;
    }
    inline void SetLabel(std::string label) {
      label_ = label;
    }
    inline ByteArray GetContent() const {
      return content_;
    }
    inline void SetContent(ByteArray content) {
      content_ = content;
    }
  private:
    uint64_t id_;
    uint64_t relId_;
    std::string label_;
    ByteArray content_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
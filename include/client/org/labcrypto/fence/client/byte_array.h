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
 
#ifndef _ORG_LABCRYPTO__FENCE__CLIENT__BYTE_ARRAY_H_
#define _ORG_LABCRYPTO__FENCE_GATE__CLIENT__BYTE_ARRAY_H_

#include <stdint.h>

#include <iomanip>
#include <string>


namespace org {
namespace labcrypto {
namespace fence {
namespace client {
  class ByteArray {
  public:
    ByteArray()
      : data_(0),
        length_(0) {
    }
    ByteArray(unsigned char *data,
              uint32_t length)
      : data_(0),
        length_(0) {
      FromByteArray(data, length);
    }
    ByteArray(const ByteArray &byteArray)
      : data_(0),
        length_(0) {
      FromByteArray(byteArray.data_, byteArray.length_);
    }
    virtual ~ByteArray() {
      if (data_) {
        delete [] data_;
      }
    }
  public:
    inline void SetValue(unsigned char *data, 
                         uint32_t length) {
      FromByteArray(data, length);
    }
    inline unsigned char* GetValue() const {
      return data_;
    }
    inline uint32_t GetLength() const {
      return length_;
    }
  public:
    inline ByteArray& operator =(const ByteArray &other) {
      FromByteArray(other.data_, other.length_);
      return *this;
    }
    friend std::ostream& operator <<(std::ostream& out, const ByteArray& obj) {
      out << "BYTE ARRAY:" << std::endl;
      bool newLineInserted = false;
      for (uint32_t i = 0; i < obj.length_; i++) {
        newLineInserted = false;
        out << std::uppercase << std::hex << "0x" << 
          std::setw(2) << std::setfill ('0') << (unsigned int)(obj.data_[i]) << " ";
        if ((i + 1) % 8 == 0) {
          out << std::endl;
          newLineInserted = true;
        }
      }
      if (!newLineInserted) {
        out << std::endl;
      }
      out << std::dec;
      return out;
    }
  private:
    inline void FromByteArray(unsigned char *data,
                              uint32_t length) {
      if (length == 0) {
        if (data_) {
          delete [] data_;
        }
        data_ = 0;
        length_ = 0;
        return;
      }
      length_ = length;
      if (data_) {
        delete [] data_;
      }
      data_ = new unsigned char[length_];
      for (uint32_t i = 0; i < length_; i++) {
        data_[i] = data[i];
      }
    }
  private:
    unsigned char *data_;
    uint32_t length_;
  };
} // END NAMESAPCE client
} // END NAMESPACE fence
} // END NAMESPACE labcrypto
} // END NAMESPACE org

#endif
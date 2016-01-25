/******************************************************************
 * Generated by Hottentot CC Generator
 * Date: 26-01-2016 12:58:11
 * Name: message_status.cc
 * Description:
 *   This file contains implementation of MessageStatus class.
 ******************************************************************/

#include <iostream>
#include <string.h>

#include "message_status.h"


namespace ir {
namespace ntnaeem {
namespace gate {
  unsigned char *
  MessageStatus::Serialize(uint32_t *length_ptr) {
    uint32_t totalLength = 0;
    uint32_t length0 = 0;
    unsigned char *data0 = statusCode_.Serialize(&length0);
    if (length0 <= 127) {
      totalLength += 1 + length0;
    } else if (length0 <= (256 * 256 - 1)) {
      totalLength += 3 + length0;
    } else if (length0 <= (256 * 256 * 256 - 1)) {
      totalLength += 3 + length0;
    }
    unsigned char *data = new unsigned char[totalLength];
    uint32_t c = 0;
    if (length0 <= 127) {
      data[c] = length0;
      c += 1;
    } else if (length0 <= (256 * 256 - 1)) {
      data[c] = 0x82;
      data[c + 1] = length0 / 256;
      data[c + 2] = length0 % 256;
      c += 3;
    } else if (length0 <= (256 * 256 * 256 - 1)) {
      data[c] = 0x83;
      data[c + 1] = length0 / (256 * 256);
      data[c + 2] = (length0 - data[c + 1] * (256 * 256)) / 256;
      data[c + 3] = length0 % (256 * 256);
      c += 4;
    }
    for (uint32_t i = 0; i < length0; i++) {
      data[c++] = data0[i];
    }
    if (c != totalLength) {
      std::cout << "Struct Serialization: Inconsistency in length of serialized byte array." << std::endl;
      exit(1);
    };
    if (length_ptr) {
      *length_ptr = totalLength;
    }
    return data;
  }
  void
  MessageStatus::Deserialize(unsigned char *data, uint32_t length) {
    uint32_t c = 0, elength = 0;
    if ((data[c] & 0x80) == 0) {
      elength = data[c];
      c++;
    } else {
      uint8_t ll = data[c] & 0x0f;
      if (ll == 2) {
        elength == data[c] * 256 + data[c + 1];
        c += 2;
      } else if (ll == 3) {
        elength == data[c] * 256 * 256 + data[c + 1] * 256 + data[c + 2];
        c += 3;
      }
    }
    statusCode_.Deserialize(data + c, elength);
    c += elength;
    if (c != length) {
      std::cout << "Struct Deserialization: Inconsistency in length of deserialized byte array." << std::endl;
      exit(1);
    };
  }
} // END OF NAMESPACE gate
} // END OF NAMESPACE ntnaeem
} // END OF NAMESPACE ir
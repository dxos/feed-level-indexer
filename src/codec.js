//
// Copyright 2020 DxOS.
//

import varint from 'varint';

export const codec = {
  encode (value) {
    if (Buffer.isBuffer(value)) {
      return value;
    }
    let size = 0;
    for (const num of value) {
      size = size + varint.encodingLength(num);
    }
    let buff = Buffer.allocUnsafe(size);
    let offset = 0;
    for (const num of value) {
      buff = varint.encode(num, buff, offset);
      offset = varint.encode.bytes;
    }
    return buff;
  },

  decode (buff) {
    const result = [];
    let len = buff.length;
    let offset = 0;
    while (len > 0) {
      result.push(varint.decode(buff, offset));
      offset = varint.decode.bytes;
      len = len - offset;
    }
    return result;
  }
};

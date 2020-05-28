//
// Copyright 2020 DxOS.
//

import sub from 'subleveldown';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';

import { codec } from './codec';

const bufToStr = buf => Buffer.isBuffer(buf) ? buf.toString('hex') : buf;

const SEP = Buffer.from('!');

export class LevelCacheSeq extends NanoresourcePromise {
  constructor (db, name, options = {}) {
    super();

    this._db = sub(db, name, { keyEncoding: 'binary', valueEncoding: codec });

    if (options.encode) this._encode = options.encode;
    if (options.decode) this._decode = options.decode;

    this._valuesBySeq = [];
    this._valuesByKey = new Map();

    this._db.on('closed', () => {
      this._valuesBySeq = [];
      this._valuesByKey.clear();
    });

    this.open().catch(err => process.nextTick(() => this.emit('error', err)));
  }

  get length () {
    return this._valuesBySeq.length;
  }

  get db () {
    return this._db;
  }

  get (key) {
    return this._valuesByKey.get(bufToStr(key));
  }

  /**
   * @async
   */
  async set (key, value) {
    await this.open();

    const keyStr = bufToStr(key);
    const oldValue = this.get(keyStr);
    const newValue = this._addValue(keyStr, {
      value,
      key,
      seq: oldValue ? oldValue.seq : this._valuesByKey.size
    });

    // TODO: Removed extra separator. Solution for now until subleveldown fix buffer keys issue.
    return this._db.put(Buffer.concat([SEP, key, SEP], key.length + 2), [newValue.seq].concat(this._encode(key, newValue.value)));
  }

  getBySeq (seq) {
    return this._valuesBySeq[seq];
  }

  async _open () {
    await this._db.open();

    for await (const item of this._db.createReadStream()) {
      const { value } = item;
      let { key } = item;
      // TODO: Removed. Solution for now until subleveldown fix buffer keys issue.
      key = key.slice(1, key.length - 1);

      if (value[0] === undefined) {
        throw new Error('missing seq number');
      }

      this._addValue(key.toString('hex'), {
        value: this._decode(key, value.slice(1)),
        key,
        seq: value[0]
      });
    }
  }

  _addValue (keyStr, value) {
    this._valuesByKey.set(keyStr, value);
    this._valuesBySeq[value.seq] = value;
    return value;
  }

  async _encode (key, value) {
    return [];
  }

  async _decode (key, value) {
    return {};
  }
}

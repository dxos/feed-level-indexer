//
// Copyright 2020 DxOS.
//

import sub from 'subleveldown';
import reachdown from 'reachdown';

import { Resource } from './resource';
import { codec } from './codec';

const bufToStr = buf => Buffer.isBuffer(buf) ? buf.toString('hex') : buf;

export class LevelCacheSeq extends Resource {
  constructor (db, name, options = {}) {
    super();

    this._db = sub(db, name, { valueEncoding: codec });
    this._prefix = reachdown(this._db, 'subleveldown').prefix;

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
    const valueEncoded = this._encode(key, value);
    value = {
      ...value,
      key,
      seq: oldValue ? oldValue.seq : this._valuesByKey.size
    };
    this._addValue(keyStr, value);
    return this._db.put(keyStr, [value.seq].concat(valueEncoded));
  }

  getBySeq (seq) {
    return this._valuesBySeq[seq];
  }

  async _open () {
    await this._db.open();

    for await (const item of this._db.createReadStream()) {
      const { key: keyStr, value } = item;

      if (value[0] === undefined) {
        throw new Error('missing seq number');
      }

      const key = Buffer.from(keyStr, 'hex');
      const valueDecoded = this._decode(key, value.slice(1));
      valueDecoded.key = key;
      valueDecoded.seq = value[0];
      this._addValue(keyStr, valueDecoded);
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

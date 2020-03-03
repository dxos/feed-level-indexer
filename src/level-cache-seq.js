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

    key = bufToStr(key);
    const oldValue = this.get(key);
    value = this._addValue(key, oldValue ? oldValue.seq : this._valuesByKey.size, value);
    return this._db.put(key, [value.seq, ...this._encode(key, value)]);
  }

  getBySeq (seq) {
    return this._valuesBySeq[seq];
  }

  async _open () {
    await this._db.open();

    for await (const item of this._db.createReadStream()) {
      const { key, value } = item;

      if (value[0] === undefined) {
        throw new Error('missing seq number');
      }

      const [seq, ..._value] = value;
      this._addValue(key, seq, this._decode(key, _value));
    }
  }

  _addValue (key, seq, value) {
    value = { ...value, key, seq };
    this._valuesByKey.set(key, value);
    this._valuesBySeq[seq] = value;
    return value;
  }

  async _encode (key, value) {
    return [];
  }

  async _decode (key, value) {
    return {};
  }
}

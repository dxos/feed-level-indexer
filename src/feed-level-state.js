//
// Copyright 2020 DxOS.
//

import through from 'through2';
import sub from 'subleveldown';
import reachdown from 'reachdown';

import { Resource } from './resource';
import { codec } from './codec';

export class FeedLevelState extends Resource {
  constructor (db) {
    super();

    this._db = sub(db, 'feed-state', { valueEncoding: codec });
    this._prefix = reachdown(this._db, 'subleveldown').prefix;
    this._feedsByInc = [];
    this._feedsByKey = new Map();
  }

  getByKey (key) {
    return this._feedsByKey.get(key.toString('hex'));
  }

  getByInc (inc) {
    return this._feedsByInc[inc];
  }

  buildIncremental () {
    return through.obj((chunk, _, next) => {
      const { key } = chunk;
      const keyHex = key.toString('hex');

      if (this._feedsByKey.has(keyHex)) {
        return next(null, chunk);
      }

      const value = { key, start: 0, inc: this._feedsByKey.size };
      this._feedsByKey.set(keyHex, value);
      this._feedsByInc[value.inc] = value;
      this._db.put(keyHex, [value.inc, value.start], err => {
        if (err) return next(err);
        next(null, chunk);
      });
    });
  }

  buildState () {
    return through.obj((chunk, _, next) => {
      const { key, seq } = chunk;
      const keyHex = key.toString('hex');

      const value = this._feedsByKey.get(keyHex);
      value.start = seq + 1;
      this._db.put(keyHex, [value.inc, value.start], err => {
        if (err) return next(err);
        next(null, chunk);
      });
    });
  }

  async _open () {
    await this._db.open();

    for await (const item of this._db.createReadStream()) {
      const { key, value: _value } = item;

      if (_value[0] === undefined) {
        throw new Error('missing inc');
      }

      const value = { inc: _value[0], start: _value[1], key: Buffer.from(key, 'hex') };

      this._feedsByKey.set(key, value);
      this._feedsByInc[value.inc] = value;
    }
  }

  async _close () {
    await this._db.close();
  }
}

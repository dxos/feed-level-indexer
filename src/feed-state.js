//
// Copyright 2020 DxOS.
//

import { EventEmitter } from 'events';

import through from 'through2';
import sub from 'subleveldown';
import reachdown from 'reachdown';

import { AsyncInitializer } from './async-initializer';
import { codec } from './codec';

export class FeedState extends EventEmitter {
  constructor (db, prefix) {
    super();

    this._db = sub(db, prefix + '.feeds', { valueEncoding: codec });
    this._prefix = reachdown(this._db, 'subleveldown').prefix;
    this._db.on('put', (key, value) => db.emit('put-sublevel', key, value, this._prefix));

    this._feedsByInc = [];
    this._feedsByKey = new Map();

    this._initializer = new AsyncInitializer(() => this._initialize());
    this._initializer.on('error', err => this.emit('error', err));
  }

  async ready () {
    return this._initializer.ready();
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
      next(null, chunk);
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

  async _initialize () {
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
}

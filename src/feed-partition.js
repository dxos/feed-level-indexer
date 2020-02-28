//
// Copyright 2020 DxOS.
//

import { EventEmitter } from 'events';

import through from 'through2';
import pump from 'pump';
import Live from 'level-live';
import sub from 'subleveldown';

import { codec } from './codec';

export class FeedPartition extends EventEmitter {
  constructor (options = {}) {
    super();

    const { db, name, feedState, getMessage } = options;

    this._db = sub(db, name, { keyEncoding: codec });
    this._feedState = feedState;
    this._getMessage = (key, seq) => getMessage(key, seq);
    this._streams = new Set();
  }

  get (key) {
    return this._db.get(key);
  }

  createReadStream (options = {}) {
    const { filter = () => true, live = false, ...levelOptions } = options;

    const reader = live ? new Live(this._db, levelOptions) : this._db.createReadStream(levelOptions);

    const throughFilter = through.obj(async (chunk, _, next) => {
      try {
        await this._feedState.ready();

        const [inc, seq] = chunk.key;
        const { key } = this._feedState.getByInc(inc);
        const data = await this._getMessage(key, seq);
        const result = await filter(data);
        if (result) {
          next(null, data);
        } else {
          next();
        }
      } catch (err) {
        next(err);
      }
    });

    const stream = pump(reader, throughFilter, () => {
      this._streams.delete(stream);
    });

    reader.on('sync', () => stream.emit('sync'));

    this._streams.add(stream);
    return stream;
  }

  add (key, seq, cb) {
    const { inc } = this._feedState.getByKey(key);
    const dbKey = codec.encode([inc, seq]);
    this._db.get(dbKey, err => {
      if (err && err.notFound) {
        this._db.put(dbKey, 1, cb);
      } else {
        cb(null);
      }
    });
  }

  destroy (err) {
    this._streams.forEach(stream => {
      if (!stream.destroyed) {
        stream.destroy(err);
      }
    });
  }
}

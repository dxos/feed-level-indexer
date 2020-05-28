//
// Copyright 2020 DxOS.
//

import through from 'through2';
import pumpify from 'pumpify';
import sub from 'subleveldown';
import eos from 'end-of-stream';
import Live from 'level-live';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';

import { codec } from './codec';

export const defaultKeyReducer = (fields) => {
  fields = fields.map(field => [field.split('.')]);

  const getValue = (data, field) => {
    for (const prop of field) {
      if (data === undefined) return data;
      data = data[prop];
    }
    return data;
  };

  return (chunk) => {
    const { data } = chunk;
    const key = [];

    for (const field of fields) {
      const value = getValue(data, field);
      if (value === undefined || value === null) {
        throw new Error('missing field values to index');
      }
      key.push(value);
    }

    return key;
  };
};

export class FeedLevelIndex extends NanoresourcePromise {
  constructor (options = {}) {
    super();

    const { db, name, keyReducer, feedState, getMessage } = options;

    this._db = sub(db, name, { keyEncoding: 'binary', valueEncoding: codec });
    this._keyReduce = Array.isArray(keyReducer) ? defaultKeyReducer(keyReducer) : keyReducer;
    this._feedState = feedState;
    this._getMessage = (key, seq) => getMessage(key, seq);
    this._streams = new Set();
    this._separator = '!';
    this._separatorBuf = Buffer.from(this._separator);
    this._ceiling = String.fromCharCode(this._separator.charCodeAt(0) + 1);
    this._ceilingBuf = Buffer.from(this._ceiling);
  }

  get db () {
    return this._db;
  }

  createReadStream (levelKey, options = {}) {
    const { filter = () => true, live = false, feedLevelIndexInfo = false, ...levelOptions } = options;

    const gte = this._encodeKey(levelKey);
    const sliceBuf = gte.slice(0, -1);
    const lte = Buffer.concat([sliceBuf, this._ceilingBuf], sliceBuf.length + this._ceilingBuf.length);

    const stream = pumpify.obj();

    const readOptions = { ...levelOptions, gte, lte };
    const reader = live ? new Live(this._db, readOptions) : this._db.createReadStream(readOptions);

    const readFeedMessage = through.obj(async (chunk, _, next) => {
      try {
        const [levelSeq, seq] = chunk.value;
        const { key } = this._feedState.getBySeq(levelSeq);
        const data = await this._getMessage(key, seq);
        const valid = await filter(data);
        if (valid) {
          if (feedLevelIndexInfo) {
            next(null, { key, seq, levelKey: chunk.key, levelSeq, data });
          } else {
            next(null, data);
          }
        } else {
          next();
        }
      } catch (err) {
        next(err);
      }
    });

    Promise.all([
      this.open(),
      this._feedState.open()
    ]).then(() => {
      stream.setPipeline(reader, readFeedMessage);
      reader.on('sync', () => stream.emit('sync'));
    }).catch((err) => {
      stream.destroy(err);
    });

    eos(stream, () => {
      this._streams.delete(stream);
    });

    this._streams.add(stream);
    return stream;
  }

  async add (chunk) {
    await this.open();
    const { key, seq } = chunk;

    const state = this._feedState.get(key);

    let dbKey = null;
    try {
      dbKey = this._keyReduce(chunk, state);
      dbKey = this._encodeKey(dbKey);
      await this._db.get(dbKey);
    } catch (err) {
      if (err.notFound) {
        return this._db.put(dbKey, [state.seq, seq]);
      }
    }
  }

  async open () {
    await this._db.open();
  }

  async close (err) {
    return Promise.all(Array.from(this._streams.values()).map(stream => {
      return new Promise(resolve => {
        if (stream.destroyed) {
          return resolve();
        }

        eos(stream, () => resolve());
        stream.destroy(err);
      });
    }));
  }

  _encodeKey (levelKey) {
    // TODO: Removed extra separator. Solution for now until subleveldown fix buffer keys issue.
    const prefix = [this._separatorBuf];
    let len = 1;

    for (let value of levelKey) {
      if (value === undefined || value === null) {
        throw new Error('the levelKey cannot contain null or undefined values');
      }

      const isBuffer = Buffer.isBuffer(value);

      if (Array.isArray(value) || (typeof value === 'object' && !isBuffer)) {
        throw new Error('the levelKey cannot contain array or object values');
      }

      if (!isBuffer) {
        if (typeof value === 'boolean') {
          value = value ? 't' : 'f';
        }
        value = Buffer.from(typeof value === 'string' ? value : String(value));
      }

      prefix.push(value);
      prefix.push(this._separatorBuf);
      len += value.length + this._separatorBuf.length;
    }

    return Buffer.concat(prefix, len);
  }
}

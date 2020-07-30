//
// Copyright 2020 DxOS.
//

import through from 'through2';
import pumpify from 'pumpify';
import sub from 'subleveldown';
import eos from 'end-of-stream';
import Live from '@frando/level-live';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';
import bufferJson from 'buffer-json-encoding';

import { batchPut } from './batch-put';

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
      let value = getValue(data, field);

      if (value === undefined || value === null) {
        value = '';
      }

      key.push(value);
    }

    return key;
  };
};

export class FeedLevelIndex extends NanoresourcePromise {
  constructor (options = {}) {
    super();

    const { db, name, keyReducer, feedState } = options;

    this._db = sub(db, name, { keyEncoding: 'binary', valueEncoding: bufferJson });
    this._name = name;
    this._keyReduce = Array.isArray(keyReducer) ? defaultKeyReducer(keyReducer) : keyReducer;
    this._feedState = feedState;
    this._batchPut = batchPut(this._db);
    this._streams = new Set();
    this._separator = '!';
    this._separatorBuf = Buffer.from(this._separator);
    this._ceiling = String.fromCharCode(this._separator.charCodeAt(0) + 1);
    this._ceilingBuf = Buffer.from(this._ceiling);
  }

  get db () {
    return this._db;
  }

  get name () {
    return this._name;
  }

  get streams () {
    return Array.from(this._streams.values());
  }

  createReadStream (levelKey, options = {}) {
    const { filter = () => true, live = false, ...levelOptions } = options;

    const gte = this._encodeKey(levelKey);
    const sliceBuf = gte.slice(0, -1);
    const lte = Buffer.concat([sliceBuf, this._ceilingBuf], sliceBuf.length + this._ceilingBuf.length);

    const stream = pumpify.obj();

    const readOptions = { ...levelOptions, gte, lte };
    const reader = live ? new Live(this._db, readOptions) : this._db.createReadStream(readOptions);

    const readFeedMessage = through.obj(async (chunk, _, next) => {
      try {
        const data = chunk.value;
        const state = this._feedState.get(data.key);
        data.levelSeq = state.seq;
        data.levelKey = chunk.key;
        const valid = await filter(data.data);
        if (valid) {
          return next(null, data);
        }

        next();
      } catch (err) {
        next(err);
      }
    });

    stream.setPipeline(reader, readFeedMessage);

    reader.once('sync', () => {
      if (this._feedState.sync) {
        stream.emit('sync');
      }
    });

    eos(stream, () => {
      this._streams.delete(stream);
    });

    this._streams.add(stream);

    return stream;
  }

  async add (chunk, batch = true) {
    await this.open();

    const { data, key, seq } = chunk;
    const state = this._feedState.get(key);
    const dbKey = this._encodeKey(this._keyReduce(chunk, state));
    if (batch) {
      return this._batchPut(dbKey, { data, key, seq });
    }
    return this._db.put(dbKey, { data, key, seq });
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
        value = Buffer.from('');
      }

      const isBuffer = Buffer.isBuffer(value);

      if (Array.isArray(value)) {
        value = Buffer.from(value.map(v => JSON.stringify(v)).join(','));
      }

      if (typeof value === 'object' && !isBuffer) {
        value = Buffer.from(JSON.stringify(value));
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

//
// Copyright 2020 DxOS.
//

import through from 'through2';
import pumpify from 'pumpify';
import sub from 'subleveldown';
import reachdown from 'reachdown';
import eos from 'end-of-stream';
import Live from 'level-live';

import { Resource } from './resource';

export class FeedLevelIndex extends Resource {
  constructor (options = {}) {
    super();

    const { db, name, fields, feedState, getMessage } = options;

    this._db = sub(db, name);
    this._prefix = reachdown(this._db, 'subleveldown').prefix;
    this._prefixReduce = this._createPrefixReducer(fields);
    this._feedState = feedState;
    this._getMessage = (key, seq) => getMessage(key, seq);
    this._streams = new Set();
    this._separator = '!';
    this._ceiling = String.fromCharCode(this._separator.charCodeAt(0) + 1);
  }

  get db () {
    return this._db;
  }

  get prefix () {
    return this._prefix;
  }

  createReadStream (prefix, options = {}) {
    const { filter = () => true, live = false, ...levelOptions } = options;

    prefix = this._encodePrefix(prefix);

    const stream = pumpify.obj();

    const readOptions = { ...levelOptions, gte: prefix, lte: prefix.slice(0, -1) + this._ceiling };
    const reader = live ? new Live(this._db, readOptions) : this._db.createReadStream(readOptions);

    const readFeedMessage = through.obj(async (chunk, _, next) => {
      try {
        const keys = chunk.key.split('!');
        const seq = keys[keys.length - 2];
        const inc = keys[keys.length - 3];
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

    const prefix = this._prefixReduce(chunk);
    const { inc } = this._feedState.getByKey(chunk.key);
    const dbKey = this._encodePrefix([...prefix, inc, chunk.seq]);
    try {
      await this._db.get(dbKey);
    } catch (err) {
      if (err.notFound) {
        return this._db.put(dbKey, '');
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

  _encodePrefix (prefix) {
    return prefix.map(value => Buffer.isBuffer(value) ? value.toString('hex') : value).join(this._separator) + this._separator;
  }

  _createPrefixReducer (fields) {
    if (typeof fields === 'string') {
      fields = [fields];
    }

    fields = fields.map(field => [field.split('.')]);

    const getValue = (data, field) => {
      for (const prop of field) {
        if (data === undefined) return data;
        data = data[prop];
      }
      return data;
    };

    const iterate = (data, prefix, missing) => {
      for (const [field, value] of prefix) {
        if (value) {
          continue;
        }

        const newValue = getValue(data, field);
        if (newValue !== undefined) {
          missing--;
          prefix.set(field, newValue);
        }
      }
      return missing;
    };

    return (chunk) => {
      const prefix = new Map(fields);
      let missing = prefix.size;

      missing = iterate(chunk.data, prefix, missing);

      if (missing && chunk.metadata) {
        missing = iterate(chunk.metadata, prefix, missing);
      }

      if (missing) {
        throw new Error('missing field values to get the prefix');
      }

      return Array.from(prefix.values());
    };
  }
}

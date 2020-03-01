//
// Copyright 2020 DxOS.
//

import through from 'through2';
import pump from 'pump';
import sub from 'subleveldown';
import reachdown from 'reachdown';
import eos from 'end-of-stream';

import { Resource } from './resource';
import { Live } from './live';
import { codec } from './codec';

const partitionCodec = {
  encode: codec.encode,
  decode (buff) {
    const lastSep = buff.lastIndexOf(33);
    if (lastSep === -1) {
      return codec.decode(buff);
    }
    return codec.decode(buff.slice(lastSep + 1));
  }
};

export class FeedPartition extends Resource {
  constructor (options = {}) {
    super();

    const { db, prefix, feedState, getMessage } = options;

    this._db = sub(db, prefix, { keyEncoding: partitionCodec });
    this._prefix = reachdown(this._db, 'subleveldown').prefix;
    this._db.on('put', (key, value) => db.emit('put-sublevel', key, value, this._prefix));
    this._db.on('put-sublevel', (key, value, prefix) => db.emit('put-sublevel', key, value, prefix));

    this._feedState = feedState;
    this._getMessage = (key, seq) => getMessage(key, seq);
    this._streams = new Set();
  }

  get db () {
    return this._db;
  }

  get prefix () {
    return this._prefix;
  }

  createReadStream (options = {}) {
    const { filter = () => true, live = false, ...levelOptions } = options;

    const stream = through.obj(async (chunk, _, next) => {
      try {
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

    Promise.all([
      this.open(),
      this._feedState.open()
    ]).then(() => {
      const reader = live ? new Live(this._db, levelOptions) : this._db.createReadStream(levelOptions);

      pump(reader, stream);

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

  async add (key, seq) {
    await this.open();

    const { inc } = this._feedState.getByKey(key);
    const dbKey = codec.encode([inc, seq]);
    try {
      await this._db.get(dbKey);
    } catch (err) {
      if (err.notFound) {
        return this._db.put(dbKey, 1);
      }
    }
  }

  async open () {
    await this._db.open();
  }

  async close (err) {
    await Promise.all(Array.from(this._streams.values()).forEach(stream => {
      if (!stream.destroyed) {
        stream.destroy(err);
        return new Promise(resolve => eos(stream, () => resolve()));
      }
    }));

    await this._db.close();
  }
}

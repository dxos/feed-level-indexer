//
// Copyright 2020 DxOS.
//

import assert from 'assert';

import level from 'level';
import pump from 'pump';
import through from 'through2';
import eos from 'end-of-stream';

import { Resource } from './resource';
import { FeedState } from './feed-state';
import { FeedPartition } from './feed-partition';
import { ERR_INDEX_FIELD_MISSING } from './errors';

export class FeedLevelIndexer extends Resource {
  constructor (options = {}) {
    super();

    const { db, indexes, source } = options;

    assert(db && (typeof db === 'string' || typeof db.supports === 'object'), 'db is required');
    assert(Array.isArray(indexes), 'indexes is required');
    assert(source, 'source is required');
    assert(typeof source.stream === 'function', 'source.stream is required');
    assert(typeof source.get === 'function', 'source.get is required');

    this._db = typeof db === 'string' ? level(db) : db;
    this._indexes = indexes;
    this._source = source;
    this._partitions = new Map();
    this._stream = null;

    this._initialize();
    this.open().catch(err => this.emit('error', err));
  }

  subscribe (prefix, options = {}) {
    const partition = this.getPartition(prefix);
    if (!partition) throw new Error('partition not found', prefix);
    return partition.createReadStream(Object.assign({}, options, { live: true }));
  }

  getPartition (prefix) {
    if (this.closed || this.closing) {
      throw new Error('indexer is closed');
    }

    if (!Array.isArray(prefix)) {
      prefix = [prefix];
    }

    prefix = prefix.map(value => Buffer.isBuffer(value) ? value.toString('hex') : value);

    const prefixKey = prefix.join('!');

    let partition;
    if (this._partitions.has(prefixKey)) {
      partition = this._partitions.get(prefixKey);
    } else {
      partition = new FeedPartition({
        db: prefix.length > 1 ? this.getPartition(prefix.slice(0, prefix.length - 1)).db : this._db,
        prefix: prefix[prefix.length - 1],
        feedState: this._feedState,
        getMessage: this._source.get
      });
      this._partitions.set(prefixKey, partition);
    }

    return partition;
  }

  _initialize () {
    this._indexes = this._indexes.map(fields => this._createIndexPrefix(fields));

    this._feedState = new FeedState(this._db);
    this._feedState.on('error', err => this.emit('error', err));

    this.on('error', (err) => {
      this.close(err).catch(err => console.error(err));
    });
  }

  async _open () {
    await this._feedState.open();

    const buildPartitions = through.obj((chunk, _, next) => {
      const { key, seq } = chunk;

      Promise.all(this._indexes.map((index) => {
        const prefix = index(chunk);
        const partition = this.getPartition(prefix);
        return partition.add(key, seq);
      })).then(() => {
        this.emit('indexed', chunk);
        next(null, chunk);
      }).catch((err) => {
        next(err);
      });
    });

    this._stream = pump(
      this._source.stream(this._feedState),
      this._feedState.buildIncremental(),
      buildPartitions,
      this._feedState.buildState(),
      err => {
        this.emit('error', err);
      });
  }

  async _close (err) {
    if (this._stream && !this._stream.destroyed) {
      this._stream.destroy(err);
      await new Promise(resolve => eos(this._stream, () => resolve()));
    }

    await Promise.all(this._partitions.map(partition => partition.close(err)));
    await this._feedState.close();
    this._partitions.clear();
  }

  _createIndexPrefix (fields) {
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
        if (newValue) {
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
        throw new ERR_INDEX_FIELD_MISSING(Array.from(prefix.keys()).reduce((prev, curr) => {
          prev[curr.join('.')] = prefix.get(curr);
          return prev;
        }, {}), chunk);
      }

      return Array.from(prefix.values());
    };
  }
}

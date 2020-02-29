//
// Copyright 2020 DxOS.
//

import { EventEmitter } from 'events';
import assert from 'assert';

import level from 'level';
import pump from 'pump';
import through from 'through2';

import { AsyncInitializer } from './async-initializer';
import { FeedState } from './feed-state';
import { FeedPartition } from './feed-partition';

export class FeedLevelIndexer extends EventEmitter {
  constructor (options = {}) {
    super();

    const { db, index, source } = options;

    assert(db && (typeof db === 'string' || typeof db.supports === 'object'), 'db is required');
    assert(Array.isArray(index), 'index is required');
    assert(source, 'source is required');
    assert(typeof source.stream === 'function', 'source.stream is required');
    assert(typeof source.get === 'function', 'source.get is required');

    this._db = typeof db === 'string' ? level(db) : db;
    this._index = this._buildIndex(index);
    this._source = source;
    this._partitions = new Map();
    this._stream = null;

    this._feedState = new FeedState(this._db, index.join('.'));
    this._feedState.on('error', err => this.emit('error', err));

    this._initializer = new AsyncInitializer(() => this._initialize());
    this._initializer.on('error', err => this.emit('error', err));

    this.on('error', (err) => {
      if (this._stream && !this._stream.destroyed) {
        this._stream.destroy(err);
      }

      this._partitions.forEach(partition => {
        partition.destroy(err);
      });
    });
  }

  async ready () {
    await this._initializer.ready();
    return this;
  }

  subscribe (prefix, options = {}) {
    const partition = this.getPartition(prefix);
    if (!partition) throw new Error('partition not found', prefix);
    return partition.createReadStream(Object.assign({}, options, { live: true }));
  }

  getPartition (prefix) {
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

  async _initialize () {
    await this._feedState.ready();

    const buildPartition = through.obj((chunk, _, next) => {
      const { key, seq } = chunk;

      const prefix = this._index(chunk);
      if (!prefix || prefix.length === 0) {
        return next(null, chunk);
      }

      const partition = this.getPartition(prefix);

      partition.add(key, seq, err => {
        if (err) return next(err);
        next(null, chunk);
      });
    });

    const indexed = through.obj((chunk, _, next) => {
      this.emit('chunk-indexed', chunk);
      next();
    });

    this._stream = pump(
      this._source.stream(this._feedState),
      this._feedState.buildIncremental(),
      buildPartition,
      this._feedState.buildState(),
      indexed,
      err => {
        this.emit('error', err);
      });
  }

  _buildIndex (fields) {
    if (typeof fields === 'string') {
      fields = [fields];
    }

    fields = fields.map(field => field.split('.'));

    const getValue = (data, field) => {
      for (const prop of field) {
        data = data[prop];
      }
      return data;
    };

    const iterate = (data, prefix) => {
      for (let i = 0; i < fields.length; i++) {
        const value = getValue(data, fields[i]);
        if (value) {
          prefix[i] = value;
        }
      }
      return prefix;
    };

    return (chunk) => {
      let prefix = iterate(chunk.data, []);

      if (chunk.metadata) {
        prefix = iterate(chunk.metadata, prefix);
      }

      prefix = prefix.filter(Boolean);

      return prefix;
    };
  }
}

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

    const { db, indexBy, source } = options;

    assert(db && (typeof db === 'string' || typeof db.supports === 'object'), 'db is required');
    assert(Array.isArray(indexBy) || ['string', 'function'].includes(typeof indexBy), 'indexBy is required');
    assert(source, 'source is required');
    assert(typeof source.stream === 'function', 'source.stream is required');
    assert(typeof source.get === 'function', 'source.get is required');

    this._db = typeof db === 'string' ? level(db) : db;
    this._indexBy = this._buildIndexBy(indexBy);
    this._source = source;
    this._partitions = new Map();
    this._stream = null;

    this._feedState = new FeedState(this._db);
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

  subscribe (partitionName, options = {}) {
    const partition = this.getPartition(partitionName);
    if (!partition) throw new Error('partition not found', partitionName);
    return partition.createReadStream(Object.assign({}, options, { live: true }));
  }

  getPartition (partitionName) {
    if (!Array.isArray(partitionName)) {
      partitionName = [partitionName];
    }

    partitionName = partitionName.map(value => Buffer.isBuffer(value) ? value.toString('hex') : value).join('!');

    let partition;
    if (this._partitions.has(partitionName)) {
      partition = this._partitions.get(partitionName);
    } else {
      partition = new FeedPartition({
        db: this._db,
        name: partitionName,
        feedState: this._feedState,
        getMessage: this._source.get
      });
      this._partitions.set(partitionName, partition);
    }
    return partition;
  }

  async _initialize () {
    await this._feedState.ready();

    const buildPartition = through.obj((chunk, _, next) => {
      const { key, seq } = chunk;

      const partitionName = this._indexBy(chunk);
      if (!partitionName) {
        return next(null, chunk);
      }

      const partition = this.getPartition(partitionName);

      partition.add(key, seq, err => {
        if (err) return next(err);
        next(null, chunk);
      });
    });

    this._stream = pump(
      this._source.stream(this._feedState),
      this._feedState.buildIncremental(),
      buildPartition,
      this._feedState.buildState(),
      err => {
        this.emit('error', err);
      });
    this._stream.on('data', data => this.emit('data', data));
  }

  _buildIndexBy (fields) {
    if (typeof fields === 'function') {
      return (chunk) => fields(chunk);
    }

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

    const iterate = (data, partitionName) => {
      for (let i = 0; i < fields.length; i++) {
        const value = getValue(data, fields[i]);
        if (value) {
          partitionName[i] = value;
        }
      }
      return partitionName;
    };

    return (chunk) => {
      let partitionName = iterate(chunk.data, []);

      if (chunk.metadata) {
        partitionName = iterate(chunk.metadata, partitionName);
      }

      partitionName = partitionName.filter(Boolean);

      if (partitionName.length !== fields.length) {
        return null;
      }

      return partitionName;
    };
  }
}

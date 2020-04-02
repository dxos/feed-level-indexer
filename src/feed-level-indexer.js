//
// Copyright 2020 DxOS.
//

import assert from 'assert';

import pumpify from 'pumpify';
import through from 'through2';
import eos from 'end-of-stream';
import { NanoresourcePromise } from 'nanoresource-promise/emitter';

import { FeedLevelState } from './feed-level-state';
import { FeedLevelIndex } from './feed-level-index';

export class FeedLevelIndexer extends NanoresourcePromise {
  constructor (db, source) {
    super();

    assert(db && db.supports, 'db is required and must be a compatible levelup database');
    assert(source, 'source is required');
    assert(typeof source.stream === 'function', 'source.stream is required');
    assert(typeof source.get === 'function', 'source.get is required');

    this._db = db;
    this._source = source;
    this._indexes = new Map();
    this._stream = null;

    this._feedState = new FeedLevelState(this._db);
    this._feedState.on('error', err => this.emit('error', err));

    this.on('error', (err) => {
      this.close(err).catch(err => console.error(err));
    });

    this._getFeedStart = this._getFeedStart.bind(this);
  }

  get db () {
    return this._db;
  }

  by (indexName, keyReducer) {
    if (this.opened || this.opening) {
      throw new Error('index can only be defined before the opening');
    }

    if (this._indexes.has(indexName)) {
      throw new Error(`index "${indexName}" already exists`);
    }

    this._indexes.set(indexName, new FeedLevelIndex({
      db: this._db,
      name: indexName,
      keyReducer,
      feedState: this._feedState,
      getMessage: this._source.get
    }));

    return this;
  }

  getIndex (indexName) {
    const index = this._indexes.get(indexName);
    if (index) {
      return index;
    }

    throw new Error(`index "${indexName}" not found`);
  }

  subscribe (indexName, prefix, options = {}) {
    const index = this.getIndex(indexName);
    return index.createReadStream(prefix, Object.assign({}, options, { live: true }));
  }

  async clear () {
    await this._closeInternalDatabases();
    await this._db.clear();
    await this._db.close();
    this._indexes.clear();
  }

  async _open () {
    await this._feedState.open();

    const buildPartitions = through.obj(async (chunk, _, next) => {
      Promise.all(Array.from(this._indexes.values()).map((index, indexName) => {
        return index.add(chunk).catch(err => {
          this.emit('index-error', err, indexName, chunk);
        });
      }))
        .then(() => {
          next(null, chunk);
        })
        .catch(err => {
          process.nextTick(() => next(err));
        });
    });

    const cleanup = through.obj((chunk, _, next) => {
      this.emit('indexed', chunk);
      // Cleaning, we throw away the value at the end
      next();
    });

    this._stream = pumpify.obj(
      this._source.stream(this._getFeedStart),
      this._feedState.buildIncremental(),
      buildPartitions,
      this._feedState.buildState(),
      cleanup
    );

    eos(this._stream, err => {
      if (err) {
        process.nextTick(() => this.emit('error', err));
      }

      this.close().catch(err => this.emit('error', err));
    });
  }

  async _close (err) {
    await this._closeInternalDatabases(err);
    await this._db.close();
    this._indexes.clear();
  }

  async _closeInternalDatabases (err) {
    await this._destroyStream(err);
    await Promise.all(Array.from(this._indexes.values()).map(index => index.close(err)));
    await this._feedState.close();
  }

  async _destroyStream (err) {
    return new Promise(resolve => {
      if (this._stream.destroyed) {
        return resolve();
      }

      eos(this._stream, () => resolve());
      this._stream.destroy(err);
    });
  }

  _getFeedStart (key) {
    const state = this._feedState.get(key);
    if (state) return state.value.start;
    return 0;
  }
}

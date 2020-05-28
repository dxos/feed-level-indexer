//
// Copyright 2020 DxOS.
//

import through from 'through2';

import { LevelCacheSeq } from './level-cache-seq';

export class FeedLevelState extends LevelCacheSeq {
  constructor (db) {
    super(db, 'feed-state');

    this._feedStream = null;
    this._feedStoreSyncState = null;
    this._synced = false;
    this._waitingFeedsSync = new Set();
  }

  get synced () {
    return this._synced;
  }

  buildIncremental () {
    return through.obj((chunk, _, next) => {
      const { key } = chunk;

      if (this.get(key)) {
        return next(null, chunk);
      }

      this.set(key, { start: 0 })
        .then(() => next(null, chunk))
        .catch(err => next(err));
    });
  }

  buildState (feedStream) {
    if (this.length === 0 && this._feedStoreSyncState === null) {
      // First build use the feedStore state to know when is synced
      this._feedStoreSyncState = {};
      feedStream.once('synced', state => {
        this._feedStoreSyncState = state;
        Object.keys(this._feedStoreSyncState).forEach(key => {
          const feedState = this.valuesByKey.get(key);
          if (feedState && feedState.seq >= this._feedStoreSyncState[key]) {
            return;
          }

          this._waitingFeedsSync.add(key);
        });

        if (this._waitingFeedsSync.size === 0) {
          this._synced = true;
          process.nextTick(() => this.emit('synced'));
        }
      });
    } else {
      this._synced = true;
    }

    return through.obj((chunk, _, next) => {
      const { key, seq } = chunk;

      this.set(key, { start: seq + 1 })
        .then(() => {
          if (this._waitingFeedsSync.size === 0) {
            return next(null, chunk);
          }

          const strKey = key.toString('hex');
          const feedStateSeq = this._feedStoreSyncState[strKey];

          if ((feedStateSeq !== undefined) && (seq >= feedStateSeq)) {
            this._waitingFeedsSync.delete(strKey);
            if (this._waitingFeedsSync.size === 0) {
              this._synced = true;
              process.nextTick(() => this.emit('synced'));
            }
          }

          next(null, chunk);
        })
        .catch(err => next(err));
    });
  }

  _encode (key, value) {
    return [value.start];
  }

  _decode (key, value) {
    return {
      start: value[0]
    };
  }
}

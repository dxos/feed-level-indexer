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
    this._sync = false;
    this._waitingFeedsSync = new Set();
  }

  get sync () {
    return this._sync;
  }

  buildIncremental () {
    return through.obj((messages, _, next) => {
      Promise.all(messages.map(message => {
        const { key } = message;

        if (this.get(key)) {
          return;
        }

        return this.set(key, { start: 0 }, messages.length > 1);
      }))
        .then(() => next(null, messages))
        .catch(err => next(err));
    });
  }

  buildState (feedStream) {
    if (this.length === 0 && this._feedStoreSyncState === null) {
      // First build use the feedStore state to know when is sync
      this._feedStoreSyncState = {};
      feedStream.once('sync', state => {
        this._feedStoreSyncState = state;
        Object.keys(this._feedStoreSyncState).forEach(key => {
          const feedState = this.valuesByKey.get(key);
          if (feedState && feedState.value.start >= this._feedStoreSyncState[key]) {
            return;
          }

          this._waitingFeedsSync.add(key);
        });

        if (this._waitingFeedsSync.size === 0) {
          this._sync = true;
          process.nextTick(() => this.emit('sync'));
        }
      });
    } else {
      this._sync = true;
    }

    return through.obj((messages, _, next) => {
      Promise.all(messages.map(message => {
        const { key, seq } = message;

        return this.set(key, { start: seq + 1 }, messages.length > 1)
          .then(() => {
            if (this._waitingFeedsSync.size === 0) {
              return;
            }

            const strKey = key.toString('hex');
            const feedStateSeq = this._feedStoreSyncState[strKey];

            if ((feedStateSeq !== undefined) && (seq >= feedStateSeq)) {
              this._waitingFeedsSync.delete(strKey);
              if (this._waitingFeedsSync.size === 0) {
                this._sync = true;
                process.nextTick(() => this.emit('sync'));
              }
            }
          });
      }))
        .then(() => next(null, messages))
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

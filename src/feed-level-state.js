//
// Copyright 2020 DxOS.
//

import through from 'through2';

import { LevelCacheSeq } from './level-cache-seq';

export class FeedLevelState extends LevelCacheSeq {
  constructor (db) {
    super(db, 'feed-state');
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

  buildState () {
    return through.obj((chunk, _, next) => {
      const { key, seq } = chunk;

      this.set(key, { start: seq + 1 })
        .then(() => next(null, chunk))
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

//
// Copyright 2020 DxOS.
//

import levelmem from 'level-mem';

import { LevelCacheSeq } from './level-cache-seq';

test('cache basic', async () => {
  const db = levelmem();
  let cache = new LevelCacheSeq(db, 'cache', {
    encode (key, value) {
      return [value.odd];
    },
    decode (key, value) {
      return {
        odd: value[0]
      };
    }
  });

  const key1 = Buffer.from('key1');
  const key2 = Buffer.from('key2');

  expect(cache.get(key1)).toBeUndefined();

  await cache.set(key1, { odd: 0 });
  await cache.set(key2, { odd: 1 });

  expect(cache.get(key1)).toEqual({ key: key1, seq: 0, odd: 0 });
  expect(cache.get(key2)).toEqual({ key: key2, seq: 1, odd: 1 });
  await cache.set(key1, { odd: 1 });
  expect(cache.get(key1)).toEqual({ key: key1, seq: 0, odd: 1 });

  await cache.db.close();

  cache = new LevelCacheSeq(db, 'cache', {
    encode (key, value) {
      return [value.odd];
    },
    decode (key, value) {
      return {
        odd: value[0]
      };
    }
  });

  await cache.open();

  expect(cache.get(key1)).toEqual({ key: key1, seq: 0, odd: 1 });
  expect(cache.get(key2)).toEqual({ key: key2, seq: 1, odd: 1 });
});

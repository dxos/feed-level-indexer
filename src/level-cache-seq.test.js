//
// Copyright 2020 DxOS.
//

import crypto from 'crypto';
import levelmem from 'level-mem';

import { LevelCacheSeq } from './level-cache-seq';

test('cache basic', async () => {
  const db = levelmem();
  let cache = new LevelCacheSeq(db, 'cache', {
    encode (key, value) {
      return [value.foo];
    },
    decode (key, value) {
      return {
        foo: value[0]
      };
    }
  });

  const keys = [...Array(30).keys()].map(() => crypto.randomBytes(32));

  expect.assertions(keys.length * 4);

  let seq = 0;
  for await (const key of keys) {
    expect(cache.get(key)).toBeUndefined();
    await cache.set(key, { foo: 0 });
    expect(cache.get(key)).toEqual({ key, seq, value: { foo: 0 } });
    await cache.set(key, { foo: 1 });
    expect(cache.get(key)).toEqual({ key, seq, value: { foo: 1 } });
    seq++;
  }

  await cache.db.close();

  cache = new LevelCacheSeq(db, 'cache', {
    encode (key, value) {
      return [value.foo];
    },
    decode (key, value) {
      return {
        foo: value[0]
      };
    }
  });

  await cache.open();

  seq = 0;
  for (const key of keys) {
    expect(cache.get(key)).toEqual({ key, seq, value: { foo: 1 } });
    seq++;
  }
});

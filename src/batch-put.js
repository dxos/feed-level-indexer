//
// Copyright 2020 DxOS.
//

import pLimit from 'p-limit';

export function batchPut (db) {
  const limit = pLimit(1);
  let batch = [];

  return function put (key, value) {
    const op = { type: 'put', key, value };

    if (batch.length > 0) {
      batch.push(op);
      return;
    }

    batch.push(op);

    return limit(async () => {
      if (batch.length === 0) return limit.clearQueue();
      await new Promise(resolve => process.nextTick(resolve));
      await db.batch(batch);
      batch = [];
    });
  };
}

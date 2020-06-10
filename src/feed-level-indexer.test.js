//
// Copyright 2020 DxOS.
//

import ram from 'random-access-memory';
import levelmem from 'level-mem';
import pify from 'pify';
import crypto from 'hypercore-crypto';
import eos from 'end-of-stream';

import { FeedStore } from '@dxos/feed-store';

import { FeedLevelIndexer } from './feed-level-indexer';

const createIndexer = async (db, fs) => {
  const source = {
    stream (getFeedStart) {
      return fs.createBatchStream(descriptor => {
        return { live: true, start: getFeedStart(descriptor.key), feedStoreInfo: true };
      });
    },
    async get (key, seq) {
      const descriptor = fs.getDescriptorByDiscoveryKey(crypto.discoveryKey(key));
      if (!descriptor) throw new Error('missing descriptor');
      const feed = descriptor.opened ? descriptor.feed : await descriptor.open();
      return pify(feed.get.bind(feed))(seq);
    }
  };

  const indexer = new FeedLevelIndexer(db, source)
    .by('TopicType', ({ data, metadata, seq }, state) => [metadata.topic, data.type, state.seq, seq])
    .by('Odd', ['odd', 'id']);

  await indexer.open();

  return indexer;
};

const waitForMessages = (stream, condition, destroy = false) => {
  return new Promise(resolve => {
    const messages = [];
    stream.on('data', ({ data }) => {
      messages.push(data.msg);
      if (condition(messages, data)) {
        if (destroy) stream.destroy();
        resolve(messages);
      }
    });
  });
};

const append = (feed, type, msg) => {
  return pify(feed.append.bind(feed))({ type, id: crypto.randomBytes(32).toString('hex'), msg, odd: !!(msg % 2) });
};

test('basic', async () => {
  const topic1 = Buffer.from('topic1');
  const topic2 = Buffer.from('topic2');

  const fs = await FeedStore.create(ram, { feedOptions: { valueEncoding: 'json' } });

  const feed1 = await fs.openFeed('/feed1', { metadata: { topic: topic1 } });
  const feed2 = await fs.openFeed('/feed2', { metadata: { topic: topic2 } });
  const feed3 = await fs.openFeed('/feed3', { metadata: { topic: topic2 } });

  await Promise.all([
    append(feed1, 'message.ChessGame', 0),
    append(feed1, 'message.ChessGame', 1),
    append(feed1, 'message.ChessGame', 2),
    append(feed3, 'message.ChessGame', 3),
    append(feed3, 'message.ChessGame', 4),
    append(feed3, 'message.Chat', 0),
    append(feed3, 'message.Chat', 1),
    append(feed2, 'message.Chat', 2),
    append(feed2, 'message.Chat', 3)
  ]);

  const indexer = await createIndexer(levelmem(), fs);
  indexer.on('error', err => console.log(err));
  indexer.on('index-error', err => console.log(err));

  const chatStream = indexer.subscribe('TopicType', [topic2, 'message.Chat']);
  const waitForSync = new Promise(resolve => chatStream.once('sync', resolve));

  const waitForChatMessages = waitForMessages(chatStream, (messages) => {
    return messages.length === 5;
  }, false);

  const waitForTopic1Messages = waitForMessages(indexer.subscribe('TopicType', [topic1]), (messages) => {
    return messages.length === 3;
  }, true);

  const waitForOddMessages = waitForMessages(indexer.subscribe('Odd', [false]), (messages) => {
    return messages.length === 6;
  }, true);

  await append(feed2, 'message.Chat', 4);

  const chatMessages = await waitForChatMessages;
  expect(chatMessages.sort()).toEqual([0, 1, 2, 3, 4]);
  await expect(waitForSync).resolves.toBeUndefined();
  chatStream.destroy();

  const topic1Messages = await waitForTopic1Messages;
  expect(topic1Messages.sort()).toEqual([0, 1, 2]);

  const oddMessages = await waitForOddMessages;
  expect(oddMessages.sort()).toEqual([0, 0, 2, 2, 4, 4]);

  const stream = indexer.subscribe('TopicType', [topic1], { feedLevelIndexInfo: true });

  const result = await new Promise(resolve => stream.on('data', data => {
    stream.destroy();
    resolve(data);
  }));

  expect(result).toEqual({
    key: feed1.key,
    seq: 0,
    levelKey: Buffer.from('!topic1!message.ChessGame!0!0!'),
    levelSeq: 0,
    data: { type: 'message.ChessGame', id: result.data.id, msg: 0, odd: false }
  });
});

const createSimpleIndexerSubscribe = async (db = levelmem()) => {
  const topic1 = Buffer.from('topic1');

  const fs = await FeedStore.create(ram, { feedOptions: { valueEncoding: 'json' } });

  const feed1 = await fs.openFeed('/feed1', { metadata: { topic: topic1 } });

  await append(feed1, 'test', 0);

  const indexer = await createIndexer(db, fs);

  const stream = indexer.subscribe('TopicType', [topic1]);
  const messages = await waitForMessages(stream, (messages) => {
    return messages.length === 1;
  });

  expect(messages).toEqual([0]);

  return { indexer, stream, fs };
};

test('close indexer', async () => {
  const onError = jest.fn();

  const { indexer, stream } = await createSimpleIndexerSubscribe();
  indexer.on('error', onError);

  // We test close
  const streamDestroyed = new Promise(resolve => eos(stream, () => resolve()));
  const toClose = indexer.close();

  await streamDestroyed;
  await expect(toClose).resolves.toBeUndefined();
  expect(onError).toHaveBeenCalledWith(new Error('premature close'));
  await expect(indexer.open()).rejects.toThrow('Resource is closed');
});

test('close feedstore -> close indexer', async () => {
  const { indexer, stream, fs } = await createSimpleIndexerSubscribe();

  // We test close
  const streamDestroyed = new Promise(resolve => eos(stream, () => resolve()));
  const toClose = fs.close();

  await streamDestroyed;
  await expect(toClose).resolves.toBeUndefined();
  expect(indexer.closed).toBe(true);
  await expect(indexer.open()).rejects.toThrow('Resource is closed');
});

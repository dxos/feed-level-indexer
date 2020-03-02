//
// Copyright 2020 DxOS.
//

import ram from 'random-access-memory';
import levelmem from 'level-mem';
import pify from 'pify';
import crypto from 'hypercore-crypto';

import { FeedStore } from '@dxos/feed-store';

import { FeedLevelIndexer } from './feed-level-indexer';

const createIndexer = (db, fs) => {
  const source = {
    stream (feedState) {
      return fs.createReadStream(descriptor => {
        const state = feedState.getByKey(descriptor.key) || { start: 0 };
        return { live: true, start: state.start, feedStoreInfo: true };
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
    .by('TopicType', ['topic', 'type'])
    .by('Odd', ['odd']);

  indexer.on('error', console.error);
  indexer.on('index-error', err => console.log(err));

  indexer.open().catch(err => console.error(err));

  return indexer;
};

const waitForMessages = (stream, condition) => {
  return new Promise(resolve => {
    const messages = [];
    stream.on('data', data => {
      messages.push(data.msg);
      if (condition(messages, data)) {
        stream.destroy();
        resolve(messages);
      }
    });
  });
};

const append = (feed, type, msg) => {
  return pify(feed.append.bind(feed))({ type, msg, odd: !!(msg % 2) });
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

  const indexer = createIndexer(levelmem(), fs);

  const waitForChatMessages = waitForMessages(indexer.subscribe('TopicType', [topic2, 'message.Chat']), (messages) => {
    return messages.length === 5;
  });

  const waitForTopic1Messages = waitForMessages(indexer.subscribe('TopicType', [topic1]), messages => {
    return messages.length === 3;
  });

  const waitForOddMessages = waitForMessages(indexer.subscribe('Odd', [false]), messages => {
    return messages.length === 6;
  });

  await append(feed2, 'message.Chat', 4);

  const chatMessages = await waitForChatMessages;
  expect(chatMessages.sort()).toEqual([0, 1, 2, 3, 4]);

  const topic1Messages = await waitForTopic1Messages;
  expect(topic1Messages.sort()).toEqual([0, 1, 2]);

  const oddMessages = await waitForOddMessages;
  expect(oddMessages.sort()).toEqual([0, 0, 2, 2, 4, 4]);
});

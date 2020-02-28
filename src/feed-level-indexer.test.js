import ram from 'random-access-memory';
import levelmem from 'level-mem';
import pify from 'pify';
import crypto from 'hypercore-crypto';

import { FeedStore } from '@dxos/feed-store';

import { FeedLevelIndexer } from './feed-level-indexer';

const createIndexer = (db, fs) => new FeedLevelIndexer({
  db,
  indexBy: ['topic', 'type'],
  source: {
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
  }
});

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

test('basic', async () => {
  const topic1 = Buffer.from('topic1');
  const topic2 = Buffer.from('topic2');

  const fs = await FeedStore.create(ram, { feedOptions: { valueEncoding: 'json' } });

  const feed1 = await fs.openFeed('/feed1', { metadata: { topic: topic1 } });
  const feed2 = await fs.openFeed('/feed2', { metadata: { topic: topic2 } });
  const feed3 = await fs.openFeed('/feed3', { metadata: { topic: topic2 } });
  const append1 = pify(feed1.append.bind(feed1));
  const append2 = pify(feed2.append.bind(feed2));
  const append3 = pify(feed3.append.bind(feed3));

  await Promise.all([
    append1({ type: 'message.ChessGame', msg: 0 }),
    append1({ type: 'message.ChessGame', msg: 1 }),
    append1({ type: 'message.ChessGame', msg: 2 }),
    append3({ type: 'message.ChessGame', msg: 3 }),
    append3({ type: 'message.ChessGame', msg: 4 }),
    append3({ type: 'message.Chat', msg: 0 }),
    append3({ type: 'message.Chat', msg: 1 }),
    append2({ type: 'message.Chat', msg: 2 }),
    append2({ type: 'message.Chat', msg: 3 })
  ]);

  let indexer = createIndexer(levelmem(), fs);

  const createChatStream = () => indexer.subscribe([topic2, 'message.Chat']);

  const waitForFiveChatMessages = waitForMessages(createChatStream(), (messages) => {
    return messages.length === 5;
  });

  await append2({ type: 'message.Chat', msg: 4 });

  const chatMessages = await waitForFiveChatMessages;
  expect(chatMessages.sort()).toEqual([0, 1, 2, 3, 4]);

  // We recreate the index
  indexer = createIndexer(levelmem(), fs);
  const sync = jest.fn();
  const chatStream = createChatStream();
  chatStream.on('sync', sync);
  await waitForMessages(chatStream, (messages) => {
    return messages.length === 5;
  });

  expect(sync).toHaveBeenCalledTimes(1);

  // We test message.ChessGame
  const chessGameStream = indexer.subscribe([topic1, 'message.ChessGame']);
  const chessGameMessages = await waitForMessages(chessGameStream, (messages) => {
    return messages.length === 3;
  });

  expect(chessGameMessages.sort()).toEqual([0, 1, 2]);
});

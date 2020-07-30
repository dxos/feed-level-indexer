//
// Copyright 2020 DXOS.org
//

const pify = require('pify');
const crypto = require('hypercore-crypto');
const level = require('level-mem');
const eos = require('end-of-stream');
const queueMicrotask = require('queue-microtask');

if (typeof window !== 'undefined') {
  process.nextTick = function (fn) {
    var args = new Array(arguments.length - 1);
    if (arguments.length > 1) {
      for (var i = 1; i < arguments.length; i++) {
        args[i - 1] = arguments[i];
      }
    }

    queueMicrotask(() => fn(...args));
  };
}

const { FeedStore } = require('@dxos/feed-store');
const { Suite } = require('@dxos/benchmark-suite');
const { createStorage } = require('@dxos/random-access-multi-storage');
const { FeedLevelIndexer } = require('.');

const DIRECTORY = '.benchmark';
const MAX_FEEDS = 5;
const MAX_TYPES = 5;
const MAX_MESSAGES_BY_TYPE = 5000;

const random = (min, max) => Math.floor(Math.random() * (max - min)) + min;

const randomAppend = feeds => (msg) => {
  const feed = feeds[random(0, feeds.length)];
  return pify(feed.append.bind(feed))(msg);
};

const topic = 'test-topic';
const types = [...Array(MAX_TYPES).keys()].map(i => `Type${i}`);

async function prepareFeeds () {
  const feedStore = new FeedStore(createStorage(`${DIRECTORY}`), { feedOptions: { valueEncoding: 'json' } });
  await feedStore.initialize();

  const feeds = await Promise.all([...Array(MAX_FEEDS).keys()].map((i) => feedStore.openFeed(`/feed${i}`, { metadata: { topic } })));

  const append = randomAppend(feeds);

  const messages = [...Array(MAX_MESSAGES_BY_TYPE).keys()];

  await Promise.all(types.map(type => Promise.all(messages.map(() => {
    return append({ type });
  }))));

  return feedStore;
}

function getMessagesFromFeedStore (fs, type) {
  let reads = 0;
  return new Promise((resolve, reject) => {
    eos(fs.createReadStream({ feedStoreInfo: true }).on('data', chunk => {
      if (topic === chunk.metadata.topic && chunk.data.type === type) {
        reads++;
      }
    }), err => {
      if (err) return reject(err);
      if (reads !== MAX_MESSAGES_BY_TYPE) return reject(Error('missing messages'));
      resolve();
    });
  });
}

const createIndexer = async (fs) => {
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

  const indexer = new FeedLevelIndexer(level(`${DIRECTORY}/db`), source)
    .by('TopicType', ({ data, metadata, seq }, state) => [metadata.topic, data.type, state.seq, seq]);

  indexer.on('error', err => console.error(err));

  await indexer.open();

  await new Promise(resolve => indexer.once('sync', resolve));

  return (type) => {
    return new Promise((resolve, reject) => {
      let reads = 0;
      const stream = indexer.subscribe('TopicType', [topic, type]);
      stream.on('error', reject);
      stream.on('data', () => {
        reads++;
        if (reads === MAX_MESSAGES_BY_TYPE) {
          stream.destroy();
          resolve();
        }
      });
    });
  };
};

// We test how long it takes to read messages from each type.
// The first type has a considerable amount of messages to provide a real example.
(async () => {
  console.log(`Indexing ${MAX_MESSAGES_BY_TYPE * MAX_TYPES} messages from ${MAX_FEEDS} feeds\n`);

  const fs = await prepareFeeds();

  const getMessagesFromIndexer = await createIndexer(fs);

  for (const type of types) {
    const suite = new Suite();
    console.log(`Get messages from ${type}\n`);

    suite.test('Indexed messages', () => getMessagesFromIndexer(type));
    suite.test('FeedStore stream messages', () => getMessagesFromFeedStore(fs, type));

    const result = await suite.run();

    suite.print(result);
  }
})();

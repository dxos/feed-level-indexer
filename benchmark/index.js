//
// Copyright 2020 Wireline, Inc.
//

const tempy = require('tempy');
const Benchmarkify = require('benchmarkify');
const pify = require('pify');
const crypto = require('hypercore-crypto');
const level = require('level');
const eos = require('end-of-stream');

const { FeedStore } = require('@dxos/feed-store');
const { FeedLevelIndexer } = require('..');

const DIRECTORY = tempy.directory();
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
  const feedStore = new FeedStore(`${DIRECTORY}/feeds`, { feedOptions: { valueEncoding: 'json' } });
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
      return fs.createReadStream(descriptor => {
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
    .by('TopicType', ['topic', 'type']);

  indexer.on('error', err => console.error(err));

  await indexer.open();

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
  const fs = await prepareFeeds();
  const getMessagesFromIndexer = await createIndexer(fs);

  const benchmark = new Benchmarkify(`Indexing ${MAX_MESSAGES_BY_TYPE * MAX_TYPES} messages from ${MAX_FEEDS} feeds`).printHeader();

  for (const type of types) {
    const bench = benchmark.createSuite(`Get messages from ${type}`, { minSamples: 1 });

    bench.add('Indexed messages', (done) => {
      getMessagesFromIndexer(type).then(done).catch(err => {
        console.error(err);
        done();
      });
    });

    bench.ref('FeedStore stream messages', (done) => {
      getMessagesFromFeedStore(fs, type).then(done).catch(err => {
        console.error(err);
        done();
      });
    });

    await bench.run();
  }
})();

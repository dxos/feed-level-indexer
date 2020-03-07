import { codec } from './codec';

test('basic codec', () => {
  const buf = codec.encode([1, 2, 3]);
  expect(Buffer.isBuffer(buf)).toBe(true);

  expect(codec.decode(buf)).toEqual([1, 2, 3]);
});

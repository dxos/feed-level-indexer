import { Resource } from './resource';

test('basic', async () => {
  const open = jest.fn();
  const close = jest.fn();

  class Basic extends Resource {
    async _open () {
      open();
    }

    async _close () {
      close();
    }
  }

  const resource = new Basic();

  await resource.open();
  await resource.close();

  expect(open).toHaveBeenCalledTimes(1);
  expect(close).toHaveBeenCalledTimes(1);
  expect(resource.open()).rejects.toThrow('Resource is closed');
});

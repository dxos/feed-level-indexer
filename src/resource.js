//
// Copyright 2020 DxOS.
//

import { EventEmitter } from 'events';
import Nanoresource from 'nanoresource';

function callbackPromise () {
  let callback;

  const promise = new Promise((resolve, reject) => {
    callback = (err, value) => {
      if (err) reject(err);
      else resolve(value);
    };
  });

  callback.promise = promise;
  return callback;
}

export class NanoresourcePromise extends Nanoresource {
  constructor (options = {}) {
    super();

    const { open, close } = options;

    this._open = (cb) => { open().then(() => cb()).catch(err => cb(err)); };
    this._close = (cb) => { close().then(() => cb()).catch(err => cb(err)); };
  }

  open () {
    const callback = callbackPromise();
    super.open(callback);
    return callback.promise;
  }

  close (allowActive = false) {
    const callback = callbackPromise();
    super.close(allowActive, callback);
    return callback.promise;
  }
}

export class Resource extends EventEmitter {
  constructor () {
    super();

    this._resource = new NanoresourcePromise({
      open: this._open.bind(this),
      close: this._close.bind(this)
    });
  }

  get opened () {
    return this._resource.opened;
  }

  get opening () {
    return this._resource.opening;
  }

  get closed () {
    return this._resource.closed;
  }

  get closing () {
    return this._resource.closing;
  }

  async open () {
    return this._resource.open();
  }

  async close () {
    return this._resource.close();
  }

  async _open () {}

  async _close () {}
}

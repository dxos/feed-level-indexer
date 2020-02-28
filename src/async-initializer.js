//
// Copyright 2020 DxOS.
//

import { EventEmitter } from 'events';
import once from 'events.once';

export class AsyncInitializer extends EventEmitter {
  constructor (initialize) {
    super();

    this._ready = false;
    this._error = null;

    initialize().then(() => {
      process.nextTick(() => {
        this._ready = true;
        this.emit('ready');
      });
    }).catch(err => {
      process.nextTick(() => {
        this._ready = false;
        this._error = err;
        this.emit('error', err);
      });
    });
  }

  async ready () {
    if (this._error) {
      return this._error;
    }

    if (this._ready) {
      return;
    }

    return once(this, 'ready');
  }
}

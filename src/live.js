//
// Copyright 2020 DxOS.
//

import LevelLive from 'level-live';

export class Live extends LevelLive {
  constructor (db, opts = {}) {
    super(db, opts);

    this.db.on('put-sublevel', this.onput);
  }

  _destroy (_, cb) {
    this.db.removeListener('put-sublevel', this.onput);
    super._destroy(_, cb);
  }
}

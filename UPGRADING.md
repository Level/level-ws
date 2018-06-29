# Upgrade Guide

This document describes breaking changes and how to upgrade. For a complete list of changes including minor and patch releases, please refer to the [changelog](CHANGELOG.md).

## v1

Dropped support for node 0.10, 0.12, 2, 3, 4 and 5.

The API only exports a single function and no longer patches `levelup`.

So if you previously did:

```js
var ws = require('level-ws')
var db = ws(level('DB'))
var stream = db.createWriteStream()
```

You should now do:

```js
var WriteStream = require('level-ws')
var db = level('DB')
var stream = WriteStream(db)
```

Also, the parameters to the stream constructor were flipped.

So if you previously did:

```js
var WriteStream = require('level-ws').WriteStream
var db = level('DB')
var stream = WriteStream({ type: 'del' }, db)
```

You should now do:

```js
var WriteStream = require('level-ws')
var db = level('DB')
var stream = WriteStream(db, { type: 'del' })
```

Internal `this.writable` and `this.readable` was removed. However, `this.writable` still exists due to inheritance, but `this.readable` is now `undefined`.

Since encodings were removed from `levelup@2` we decided to remove them from `level-ws` as well.

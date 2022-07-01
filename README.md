# level-ws

**A basic writable stream for [`abstract-level`](https://github.com/Level/abstract-level) databases, using Node.js core streams.** This is not a high-performance stream. If benchmarking shows that your particular usage does not fit then try one of the alternative writable streams that are optimized for different use cases.

> :pushpin: To instead write data using Web Streams, see [`level-web-stream`](https://github.com/Level/web-stream).

[![level badge][level-badge]](https://github.com/Level/awesome)
[![npm](https://img.shields.io/npm/v/level-ws.svg)](https://www.npmjs.com/package/level-ws)
[![Node version](https://img.shields.io/node/v/level-ws.svg)](https://www.npmjs.com/package/level-ws)
[![Test](https://img.shields.io/github/workflow/status/Level/level-ws/Test?label=test)](https://github.com/Level/level-ws/actions/workflows/test.yml)
[![Coverage](https://img.shields.io/codecov/c/github/Level/level-ws?label=\&logo=codecov\&logoColor=fff)](https://codecov.io/gh/Level/level-ws)
[![Standard](https://img.shields.io/badge/standard-informational?logo=javascript\&logoColor=fff)](https://standardjs.com)
[![Common Changelog](https://common-changelog.org/badge.svg)](https://common-changelog.org)
[![Donate](https://img.shields.io/badge/donate-orange?logo=open-collective\&logoColor=fff)](https://opencollective.com/level)

## Usage

_If you are upgrading: please see [`UPGRADING.md`](UPGRADING.md)._

```js
const { Level } = require('level')
const WriteStream = require('level-ws')

const db = new Level('./db', { valueEncoding: 'json' })
const ws = new WriteStream(db)

ws.on('close', function () {
  console.log('Done!')
})

ws.write({ key: 'alice', value: 42 })
ws.write({ key: 'bob', value: 7 })

// To delete entries, specify an explicit type
ws.write({ type: 'del', key: 'tomas' })
ws.write({ type: 'put', key: 'sara', value: 16 })

ws.end()
```

## API

### `ws = new WriteStream(db[, options])`

Create a [writable stream](https://nodejs.org/dist/latest-v18.x/docs/api/stream.html#stream_class_stream_writable) that operates in object mode, accepting batch operations to be committed with `db.batch()` on each tick of the Node.js event loop. The optional `options` argument may contain:

- `type` (string, default: `'put'`): default batch operation type if not set on indididual operations.
- `maxBufferLength` (number, default `Infinity`): limit the size of batches. When exceeded, the stream will stop processing writes until the current batch has been committed.
- `highWaterMark` (number, default `16`): buffer level when `stream.write()` starts returning false.

## Contributing

[`Level/level-ws`](https://github.com/Level/level-ws) is an **OPEN Open Source Project**. This means that:

> Individuals making significant and valuable contributions are given commit-access to the project to contribute as they see fit. This project is more like an open wiki than a standard guarded open source project.

See the [Contribution Guide](https://github.com/Level/community/blob/master/CONTRIBUTING.md) for more details.

## License

[MIT](LICENSE)

[level-badge]: https://leveljs.org/img/badge.svg

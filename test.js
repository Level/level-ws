'use strict'

const tape = require('tape')
const { Level } = require('level')
const WriteStream = require('.')
const secretListener = require('secret-event-listener')
const tempy = require('tempy')

function monitor (stream) {
  const order = []

  ;['error', 'finish', 'close'].forEach(function (event) {
    secretListener(stream, event, function () {
      order.push(event)
    })
  })

  return order
}

function monkeyBatch (db, fn) {
  const original = db._batch.bind(db)
  db._batch = fn.bind(db, original)
}

function slowdown (db) {
  monkeyBatch(db, function (original, ops, options, cb) {
    setTimeout(function () {
      original(ops, options, cb)
    }, 500)
  })
}

function entryKv (entry) {
  return { key: entry[0], value: entry[1] }
}

function test (label, options, fn) {
  if (typeof options === 'function') {
    fn = options
    options = {}
  }

  options.createIfMissing = true
  options.errorIfExists = true

  tape(label, function (t) {
    const ctx = {}

    const sourceData = ctx.sourceData = []
    for (let i = 0; i < 2; i++) {
      ctx.sourceData.push({ key: String(i), value: 'value' })
    }

    ctx.verify = function (ws, done, data) {
      ctx.db.iterator().all(function (err, result) {
        t.error(err, 'no error')
        t.same(result.map(entryKv), data || sourceData, 'correct data')
        done()
      })
    }

    ctx.db = new Level(tempy.directory(), options)

    ctx.db.open(function (err) {
      t.ifError(err, 'no open() error')
      fn(t, ctx, function () {
        ctx.db.close(function (err) {
          t.ifError(err, 'no close() error')
          t.end()
        })
      })
    })
  })
}

// TODO: test various encodings

test('test simple WriteStream', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))
  ctx.sourceData.forEach(function (d) {
    ws.write(d)
  })
  ws.end()
})

test('test WriteStream with async writes', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)
  const sourceData = ctx.sourceData
  let i = -1

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))

  function write () {
    if (++i >= sourceData.length) return ws.end()

    const d = sourceData[i]
    // some should batch() and some should put()
    if (d.key % 3) {
      setTimeout(function () {
        ws.write(d)
        process.nextTick(write)
      }, 10)
    } else {
      ws.write(d)
      process.nextTick(write)
    }
  }

  write()
})

test('race condition between batch callback and close event', function (t, ctx, done) {
  // Delaying the batch should not be a problem
  slowdown(ctx.db)

  const ws = new WriteStream(ctx.db)
  let i = 0

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))
  ctx.sourceData.forEach(function (d) {
    i++
    if (i < ctx.sourceData.length) {
      ws.write(d)
    } else {
      ws.end(d)
    }
  })
})

test('race condition between two flushes', function (t, ctx, done) {
  slowdown(ctx.db)

  const ws = new WriteStream(ctx.db)
  const order = monitor(ws)

  ws.on('close', function () {
    t.same(order, ['batch', 'batch', 'close'])

    ctx.verify(ws, done, [
      { key: 'a', value: 'a' },
      { key: 'b', value: 'b' }
    ])
  })

  ctx.db.on('batch', function () {
    order.push('batch')
  })

  ws.write({ key: 'a', value: 'a' })

  // Schedule another flush while the first is in progress
  ctx.db.once('batch', function (ops) {
    ws.end({ key: 'b', value: 'b' })
  })
})

test('test end accepts data', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)
  let i = 0

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))
  ctx.sourceData.forEach(function (d) {
    i++
    if (i < ctx.sourceData.length) {
      ws.write(d)
    } else {
      ws.end(d)
    }
  })
})

test('test destroy()', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)

  const verify = function () {
    ctx.db.iterator().all(function (err, result) {
      t.error(err, 'no error')
      t.same(result.map(entryKv), [], 'results should be empty')
      done()
    })
  }

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', verify.bind(null))
  ctx.sourceData.forEach(function (d) {
    ws.write(d)
  })
  ws.destroy()
})

test('test destroy(err)', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)
  const order = monitor(ws)

  ws.on('error', function (err) {
    t.is(err.message, 'user error', 'got error')
  })

  ws.on('close', function () {
    t.same(order, ['error', 'close'])

    ctx.db.iterator().all(function (err, result) {
      t.error(err, 'no error')
      t.same(result.map(entryKv), [], 'results should be empty')
      done()
    })
  })

  ctx.sourceData.forEach(function (d) {
    ws.write(d)
  })

  ws.destroy(new Error('user error'))
})

test('test json encoding', { keyEncoding: 'utf8', valueEncoding: 'json' }, function (t, ctx, done) {
  const data = [
    { key: 'aa', value: { a: 'complex', obj: 100 } },
    { key: 'ab', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'ac', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ba', value: { a: 'complex', obj: 100 } },
    { key: 'bb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'bc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ca', value: { a: 'complex', obj: 100 } },
    { key: 'cb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'cc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } }
  ]

  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done, data))
  data.forEach(function (d) {
    ws.write(d)
  })
  ws.end()
})

test('test del capabilities for each key/value', { keyEncoding: 'utf8', valueEncoding: 'json' }, function (t, ctx, done) {
  const data = [
    { key: 'aa', value: { a: 'complex', obj: 100 } },
    { key: 'ab', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'ac', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ba', value: { a: 'complex', obj: 100 } },
    { key: 'bb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'bc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ca', value: { a: 'complex', obj: 100 } },
    { key: 'cb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'cc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } }
  ]

  function del () {
    const delStream = new WriteStream(ctx.db)
    delStream.on('error', function (err) {
      t.notOk(err, 'no error')
    })
    delStream.on('close', function () {
      verify()
    })
    data.forEach(function (d) {
      d.type = 'del'
      delStream.write(d)
    })

    delStream.end()
  }

  function verify () {
    ctx.db.iterator().all(function (err, result) {
      t.error(err, 'no error')
      t.same(result.map(entryKv), [], 'results should be empty')
      done()
    })
  }

  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', function () {
    del()
  })
  data.forEach(function (d) {
    ws.write(d)
  })
  ws.end()
})

test('test del capabilities as constructor option', { keyEncoding: 'utf8', valueEncoding: 'json' }, function (t, ctx, done) {
  const data = [
    { key: 'aa', value: { a: 'complex', obj: 100 } },
    { key: 'ab', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'ac', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ba', value: { a: 'complex', obj: 100 } },
    { key: 'bb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'bc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ca', value: { a: 'complex', obj: 100 } },
    { key: 'cb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'cc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } }
  ]

  function del () {
    const delStream = new WriteStream(ctx.db, { type: 'del' })
    delStream.on('error', function (err) {
      t.notOk(err, 'no error')
    })
    delStream.on('close', function () {
      verify()
    })
    data.forEach(function (d) {
      delStream.write(d)
    })

    delStream.end()
  }

  function verify () {
    ctx.db.iterator().all(function (err, result) {
      t.error(err, 'no error')
      t.same(result.map(entryKv), [], 'results should be empty')
      done()
    })
  }

  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', function () {
    del()
  })
  data.forEach(function (d) {
    ws.write(d)
  })
  ws.end()
})

test('test type at key/value level must take precedence on the constructor', { keyEncoding: 'utf8', valueEncoding: 'json' }, function (t, ctx, done) {
  const data = [
    { key: 'aa', value: { a: 'complex', obj: 100 } },
    { key: 'ab', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'ac', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ba', value: { a: 'complex', obj: 100 } },
    { key: 'bb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'bc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } },
    { key: 'ca', value: { a: 'complex', obj: 100 } },
    { key: 'cb', value: { b: 'foo', bar: [1, 2, 3] } },
    { key: 'cc', value: { c: 'w00t', d: { e: [0, 10, 20, 30], f: 1, g: 'wow' } } }
  ]
  const exception = data[0]

  exception.type = 'put'

  function del () {
    const delStream = new WriteStream(ctx.db, { type: 'del' })
    delStream.on('error', function (err) {
      t.notOk(err, 'no error')
    })
    delStream.on('close', function () {
      verify()
    })
    data.forEach(function (d) {
      delStream.write(d)
    })

    delStream.end()
  }

  function verify () {
    ctx.db.iterator().all(function (err, result) {
      t.error(err, 'no error')
      const expected = [{ key: data[0].key, value: data[0].value }]
      t.same(result.map(entryKv), expected, 'only one element')
      done()
    })
  }

  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', function () {
    del()
  })
  data.forEach(function (d) {
    ws.write(d)
  })
  ws.end()
})

test('test that missing type errors', function (t, ctx, done) {
  const data = { key: 314, type: 'foo' }
  let errored = false

  function verify () {
    ctx.db.get(data.key, function (err, value) {
      t.equal(errored, true, 'error received in stream')
      t.ok(err, 'got expected error')
      t.equal(err.notFound, true, 'not found error')
      t.notOk(value, 'did not get value')
      done()
    })
  }

  const ws = new WriteStream(ctx.db)
  ws.on('error', function (err) {
    t.equal(err.message, 'A batch operation must have a type property that is \'put\' or \'del\'', 'should error')
    errored = true
  })
  ws.on('close', function () {
    verify()
  })
  ws.write(data)
  ws.end()
})

test('test limbo batch error', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db)
  const order = monitor(ws)

  monkeyBatch(ctx.db, function (original, ops, options, cb) {
    process.nextTick(cb, new Error('batch error'))
  })

  ws.on('error', function (err) {
    t.is(err.message, 'batch error')
  })

  ws.on('close', function () {
    t.same(order, ['error', 'close'])
    t.end()
  })

  // Don't end(), because we want the error to follow a
  // specific code path (when there is no _flush listener).
  ws.write({ key: 'a', value: 'a' })
})

test('test batch error when buffer is full', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db, { maxBufferLength: 1 })
  const order = monitor(ws)

  monkeyBatch(ctx.db, function (original, ops, options, cb) {
    process.nextTick(cb, new Error('batch error'))
  })

  ws.on('error', function (err) {
    t.is(err.message, 'batch error', 'got error')
  })

  ws.on('close', function () {
    t.same(order, ['error', 'close'])
    t.end()
  })

  // Don't end(), because we want the error to follow a
  // specific code path (when we're waiting to drain).
  ws.write({ key: 'a', value: 'a' })
  ws.write({ key: 'b', value: 'b' })
})

test('test destroy while waiting to drain', function (t, ctx, done) {
  const ws = new WriteStream(ctx.db, { maxBufferLength: 1 })
  const order = monitor(ws)

  ws.on('error', function (err) {
    t.is(err.message, 'user error', 'got error')
  })

  ws.on('close', function () {
    t.same(order, ['error', 'close'])
    t.end()
  })

  ws.prependListener('_flush', function (err) {
    t.ifError(err, 'no _flush error')
    ws.destroy(new Error('user error'))
  })

  // Don't end.
  ws.write({ key: 'a', value: 'a' })
  ws.write({ key: 'b', value: 'b' })
})

;[0, 1, 2, 10, 20, 100].forEach(function (max) {
  test('test maxBufferLength: ' + max, testMaxBuffer(max, false))
  test('test maxBufferLength: ' + max + ' (random)', testMaxBuffer(max, true))
})

function testMaxBuffer (max, randomize) {
  return function (t, ctx, done) {
    const ws = new WriteStream(ctx.db, { maxBufferLength: max })
    const sourceData = []
    const batches = []

    for (let i = 0; i < 20; i++) {
      sourceData.push({ key: i < 10 ? '0' + i : String(i), value: 'value' })
    }

    const expectedSize = max || sourceData.length
    const remaining = sourceData.slice()

    ws.on('close', function () {
      t.ok(batches.every(function (size, index) {
        // Last batch may contain additional items
        return size <= expectedSize || index === batches.length - 1
      }), 'batch sizes are <= max')

      ctx.verify(ws, done, sourceData)
    })

    ctx.db.on('batch', function (ops) {
      batches.push(ops.length)
    })

    loop()

    function loop () {
      const toWrite = randomize
        ? Math.floor(Math.random() * remaining.length + 1)
        : remaining.length

      remaining.splice(0, toWrite).forEach(function (d) {
        ws.write(d)
      })

      if (remaining.length) {
        setImmediate(loop)
      } else {
        ws.end()
      }
    }
  }
}

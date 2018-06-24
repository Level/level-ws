var after = require('after'),
  tape = require('tape'),
  path = require('path'),
  fs = require('fs'),
  level = require('level'),
  rimraf = require('rimraf'),
  ws = require('./')

function cleanup (callback) {
  fs.readdir(__dirname, function (err, list) {
    if (err) return callback(err)

    list = list.filter(function (f) {
      return (/^_level-ws_test_db\./).test(f)
    })

    var done = after(list.length, callback)
    list.forEach(function (f) {
      rimraf(path.join(__dirname, f), done)
    })
  })
}

function openTestDatabase (options, callback) {
  var location = path.join(__dirname, '_level-ws_test_db.' + Math.random())
  rimraf(location, function (err) {
    if (err) return callback(err)
    level(location, options, callback)
  })
}

function test (label, options, fn) {
  if (typeof options === 'function') {
    fn = options
    options = {}
  }

  options.createIfMissing = true
  options.errorIfExists = true

  tape(label, function (t) {
    var ctx = {}

    var sourceData = ctx.sourceData = []
    for (var i = 0; i < 10; i++) {
      ctx.sourceData.push({ type: 'put', key: i, value: Math.random() })
    }

    ctx.verify = function (ws, done, data) {
      // can pass alternative data array for verification
      data = data || sourceData
      t.ok(ws.writable === false, 'not writable')
      t.ok(ws.readable === false, 'not readable')
      var _done = after(data.length, done)
      data.forEach(function (data) {
        ctx.db.get(data.key, function (err, value) {
          t.notOk(err, 'no error')
          if (typeof value === 'object') {
            t.deepEqual(value, data.value, 'WriteStream data #' + data.key + ' has correct value')
          } else {
            t.equal(+value, +data.value, 'WriteStream data #' + data.key + ' has correct value')
          }
          _done()
        })
      })
    }

    openTestDatabase(options, function (err, db) {
      t.notOk(err, 'no error')
      ctx.db = ws(db)
      fn(t, ctx, function () {
        ctx.db.close(function (err) {
          t.notOk(err, 'no error')
          cleanup(function (err) {
            t.notOk(err, 'no error')
            t.end()
          })
        })
      })
    })
  })
}

// TODO: test various encodings

test('test simple WriteStream', function (t, ctx, done) {
  var ws = ctx.db.createWriteStream()
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
  var ws = ctx.db.createWriteStream()
  var sourceData = ctx.sourceData
  var i = -1

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))

  function write () {
    if (++i >= sourceData.length) return ws.end()

    var d = sourceData[i]
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

test('test end accepts data', function (t, ctx, done) {
  var ws = ctx.db.createWriteStream()
  var i = 0

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

// at the moment, destroySoon() is basically just end()
test('test destroySoon()', function (t, ctx, done) {
  var ws = ctx.db.createWriteStream()
  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  ws.on('close', ctx.verify.bind(ctx, ws, done))
  ctx.sourceData.forEach(function (d) {
    ws.write(d)
  })
  ws.destroySoon()
})

test('test destroy()', function (t, ctx, done) {
  var ws = ctx.db.createWriteStream()

  var verify = function () {
    t.ok(ws.writable === false, 'not writable')
    var _done = after(ctx.sourceData.length, done)
    ctx.sourceData.forEach(function (data) {
      ctx.db.get(data.key, function (err, value) {
        // none of them should exist
        t.ok(err, 'got expected error')
        t.notOk(value, 'did not get value')
        _done()
      })
    })
  }

  ws.on('error', function (err) {
    t.notOk(err, 'no error')
  })
  t.ok(ws.writable === true, 'is writable')
  t.ok(ws.readable === false, 'not readable')
  ws.on('close', verify.bind(null))
  ctx.sourceData.forEach(function (d) {
    ws.write(d)
    t.ok(ws.writable === true, 'is writable')
    t.ok(ws.readable === false, 'not readable')
  })
  t.ok(ws.writable === true, 'is writable')
  t.ok(ws.readable === false, 'not readable')
  ws.destroy()
})

test('test json encoding', { keyEncoding: 'utf8', valueEncoding: 'json' }, function (t, ctx, done) {
  var data = [
    { type: 'put', key: 'aa', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'ab', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'ac', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { type: 'put', key: 'ba', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'bb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'bc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { type: 'put', key: 'ca', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'cb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'cc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } }
  ]

  var ws = ctx.db.createWriteStream()
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
  var data = [
    { type: 'put', key: 'aa', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'ab', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'ac', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { type: 'put', key: 'ba', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'bb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'bc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { type: 'put', key: 'ca', value: { a: 'complex', obj: 100 } },
    { type: 'put', key: 'cb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { type: 'put', key: 'cc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } }
  ]

  function del () {
    var delStream = ctx.db.createWriteStream()
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
    var _done = after(data.length, done)
    data.forEach(function (data) {
      ctx.db.get(data.key, function (err, value) {
        // none of them should exist
        t.ok(err, 'got expected error')
        t.notOk(value, 'did not get value')
        _done()
      })
    })
  }

  var ws = ctx.db.createWriteStream()
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
  var data = [
    { key: 'aa', value: { a: 'complex', obj: 100 } },
    { key: 'ab', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { key: 'ac', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { key: 'ba', value: { a: 'complex', obj: 100 } },
    { key: 'bb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { key: 'bc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
    { key: 'ca', value: { a: 'complex', obj: 100 } },
    { key: 'cb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
    { key: 'cc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } }
  ]

  function del () {
    var delStream = ctx.db.createWriteStream({ type: 'del' })
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
    var _done = after(data.length, done)
    data.forEach(function (data) {
      ctx.db.get(data.key, function (err, value) {
        // none of them should exist
        t.ok(err, 'got expected error')
        t.notOk(value, 'did not get value')
        _done()
      })
    })
  }

  var ws = ctx.db.createWriteStream()
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
  var data = [
      { key: 'aa', value: { a: 'complex', obj: 100 } },
      { key: 'ab', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
      { key: 'ac', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
      { key: 'ba', value: { a: 'complex', obj: 100 } },
      { key: 'bb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
      { key: 'bc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } },
      { key: 'ca', value: { a: 'complex', obj: 100 } },
      { key: 'cb', value: { b: 'foo', bar: [ 1, 2, 3 ] } },
      { key: 'cc', value: { c: 'w00t', d: { e: [ 0, 10, 20, 30 ], f: 1, g: 'wow' } } }
    ],
    exception = data[0]

  exception['type'] = 'put'

  function del () {
    var delStream = ctx.db.createWriteStream({ type: 'del' })
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
    var _done = after(data.length, done)
    data.forEach(function (data) {
      ctx.db.get(data.key, function (err, value) {
        if (data.type === 'put') {
          t.ok(value, 'got value')
          _done()
        } else {
          t.ok(err, 'got expected error')
          t.notOk(value, 'did not get value')
          _done()
        }
      })
    })
  }

  var ws = ctx.db.createWriteStream()
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
  var self = this
  var data = { key: 314, type: 'foo' }
  var errored = false

  function verify () {
    ctx.db.get(data.key, function (err, value) {
      t.equal(errored, true, 'error received in stream')
      t.ok(err, 'got expected error')
      t.equal(err.notFound, true, 'not found error')
      t.notOk(value, 'did not get value')
      done()
    })
  }

  var ws = ctx.db.createWriteStream()
  ws.on('error', function (err) {
    t.equal(err.message, '`type` must be \'put\' or \'del\'', 'should error')
    errored = true
  })
  ws.on('close', function () {
    verify()
  })
  ws.write(data)
  ws.end()
})

var Writable = require('readable-stream').Writable
var inherits = require('inherits')
// TODO remove
var extend = require('xtend')

var defaultOptions = {
  type: 'put',
  // TODO remove encodings, no longer part of levelup
  keyEncoding: 'utf8',
  valueEncoding: 'utf8'
}

// TODO remove, no longer part of levelup
// copied from LevelUP
var encodingNames = [
  'hex',
  'utf8',
  'utf-8',
  'ascii',
  'binary',
  'base64',
  'ucs2',
  'ucs-2',
  'utf16le',
  'utf-16le'
]

// TODO remove
// copied from LevelUP
var encodingOpts = (function () {
  var eo = {}
  encodingNames.forEach(function (e) {
    eo[e] = { valueEncoding: e }
  })
  return eo
}())

// TODO remove
// copied from LevelUP
function getOptions (levelup, options) {
  var s = typeof options === 'string' // just an encoding
  if (!s && options && options.encoding && !options.valueEncoding) {
    options.valueEncoding = options.encoding
  }
  return extend(
    (levelup && levelup.options) || {}
    , s ? encodingOpts[options] || encodingOpts[defaultOptions.valueEncoding]
      : options
  )
}

// TODO flip parameters
function WriteStream (options, db) {
  if (!(this instanceof WriteStream)) {
    return new WriteStream(options, db)
  }

  Writable.call(this, { objectMode: true })

  // TODO use Object.assign()
  this._options = extend(defaultOptions, getOptions(db, options))
  this._db = db
  this._buffer = []

  var self = this

  this.on('finish', function () {
    self.emit('close')
  })
}

inherits(WriteStream, Writable)

WriteStream.prototype._write = function write (d, enc, next) {
  var self = this
  // TODO use self.destroyed
  if (self._destroyed) return

  // TODO remove, no longer needed
  if (!self._db.isOpen()) {
    return self._db.once('ready', function () {
      write.call(self, d, enc, next)
    })
  }

  function push (d) {
    self._buffer.push(d)
    next()
  }

  if (self._options.maxBufferLength &&
      self._options.maxBufferLength === self._buffer.length) {
    self._batch(function (err) {
      if (err) return next(err)
      push(d)
    })
  } else {
    push(d)
  }
}

WriteStream.prototype._final = function (cb) {
  this._batch(cb)
}

WriteStream.prototype._batch = function (cb) {
  var self = this
  var buffer = self._buffer

  // TODO use self.destroyed
  // TODO remove !buffer check
  if (self._destroyed || !buffer) return cb()

  // TODO remove, no longer needed
  if (!self._db.isOpen()) {
    return self._db.once('ready', function () { self._final(cb) })
  }

  self._buffer = []

  // TODO remove .map(), better to push objects during _write()
  buffer = buffer.map(function (d) {
    return {
      type: d.type || self._options.type,
      key: d.key,
      value: d.value,
      // TODO remove encodings
      keyEncoding: d.keyEncoding || self._options.keyEncoding,
      valueEncoding: (d.valueEncoding || d.encoding ||
                      self._options.valueEncoding)
    }
  })

  self._db.batch(buffer, cb)
}

WriteStream.prototype.toString = function () {
  return 'LevelUP.WriteStream'
}

// TODO remove, should be enough to use default, alternatively
// implement WriteStream.prototype._destroy
WriteStream.prototype.destroy = function () {
  if (this._destroyed) return
  this._buffer = null
  this._destroyed = true
  this.emit('close')
}

WriteStream.prototype.destroySoon = function () {
  this.end()
}

// TODO remove, only export constructor
module.exports = function (db) {
  db.writeStream = db.createWriteStream = function (options) {
    return new WriteStream(options, db)
  }
  return db
}

module.exports.WriteStream = WriteStream

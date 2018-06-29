var Writable = require('readable-stream').Writable
var inherits = require('inherits')
var extend = require('xtend')

var defaultOptions = { type: 'put' }

function WriteStream (db, options) {
  if (!(this instanceof WriteStream)) {
    return new WriteStream(db, options)
  }

  Writable.call(this, { objectMode: true })

  this._options = extend(defaultOptions, options)
  this._db = db
  this._buffer = []

  var self = this

  this.on('finish', function f () {
    if (self._buffer && self._buffer.length) {
      return self._flush(f)
    }
    self.emit('close')
  })
}

inherits(WriteStream, Writable)

WriteStream.prototype._write = function (d, enc, next) {
  var self = this
  if (self.destroyed) return

  if (self._options.maxBufferLength &&
      self._buffer.length > self._options.maxBufferLength) {
    self.once('_flush', next)
  } else {
    if (self._buffer.length === 0) {
      process.nextTick(function () { self._flush() })
    }
    self._buffer.push(d)
    next()
  }
}

WriteStream.prototype._flush = function (f) {
  var self = this
  var buffer = self._buffer

  if (self.destroyed || !buffer) return

  self._buffer = []

  self._db.batch(buffer.map(function (d) {
    return {
      type: d.type || self._options.type,
      key: d.key,
      value: d.value,
      keyEncoding: d.keyEncoding || self._options.keyEncoding,
      valueEncoding: (d.valueEncoding || d.encoding ||
                      self._options.valueEncoding)
    }
  }), cb)

  function cb (err) {
    if (err) {
      self.emit('error', err)
    } else {
      if (f) f()
      self.emit('_flush')
    }
  }
}

WriteStream.prototype.toString = function () {
  return 'LevelUP.WriteStream'
}

WriteStream.prototype._destroy = function (err, cb) {
  this._buffer = null
  this.emit('close')
  cb(err)
}

WriteStream.prototype.destroySoon = function () {
  this.end()
}

module.exports = WriteStream

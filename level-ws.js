var Writable = require('flush-write-stream')
var inherits = require('inherits')
var extend = require('xtend')

var defaultOptions = {
  type: 'put',
  keyEncoding: 'utf8',
  valueEncoding: 'utf8'
}

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

// copied from LevelUP
var encodingOpts = (function () {
  var eo = {}
  encodingNames.forEach(function (e) {
    eo[e] = { valueEncoding: e }
  })
  return eo
}())

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

function WriteStream (options, db) {
  if (!(this instanceof WriteStream)) {
    return new WriteStream(options, db)
  }

  Writable.call(this, { objectMode: true }, write.bind(this), flush.bind(this))

  this._options = extend(defaultOptions, getOptions(db, options))
  this._db = db
  this._buffer = []
  this.writable = true
  this.readable = false

  var self = this

  this.on('finish', function () {
    self.writable = false
    self.emit('close')
  })
}

inherits(WriteStream, Writable)

function write (d, enc, next) {
  var self = this
  if (self.destroyed) return

  if (self._options.maxBufferLength &&
      self._buffer.length > self._options.maxBufferLength) {
    self.once('_flush', next)
  } else {
    // TODO this doesn't seem to make any difference, keeping for now
    if (self._buffer.length === 0) {
      process.nextTick(function () { self._flush() })
    }
    self._buffer.push(d)
    next()
  }
}

function flush (f) {
  var self = this
  if (self.destroyed) return

  var buffer = self._buffer
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
      self.writable = false
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

WriteStream.prototype.destroy = function () {
  if (this.destroyed) return
  this._buffer = null
  this.destroyed = true
  this.writable = false
  this.emit('close')
}

WriteStream.prototype.destroySoon = function () {
  this.end()
}

module.exports = function (db) {
  db.writeStream = db.createWriteStream = function (options) {
    return new WriteStream(options, db)
  }
  return db
}

module.exports.WriteStream = WriteStream

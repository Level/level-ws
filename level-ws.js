/* Copyright (c) 2013 LevelUP contributors
 * See list at <https://github.com/rvagg/node-levelup#contributing>
 * MIT +no-false-attribs License
 * <https://github.com/Level/level-ws/master/LICENSE>
 */

var Writable = require('stream').Writable || require('readable-stream').Writable
  , inherits = require('util').inherits
  , extend   = require('xtend')

  , defaultOptions = {
        type          : 'put'
      , keyEncoding   : 'utf8'
      , valueEncoding : 'utf8'
    }

    // copied from LevelUP
  , encodingNames  = [
        'hex'
      , 'utf8'
      , 'utf-8'
      , 'ascii'
      , 'binary'
      , 'base64'
      , 'ucs2'
      , 'ucs-2'
      , 'utf16le'
      , 'utf-16le'
    ]

    // copied from LevelUP
  , encodingOpts = (function () {
      var eo = {}
      encodingNames.forEach(function (e) {
        eo[e] = { valueEncoding : e }
      })
      return eo
    }())

// copied from LevelUP
function getOptions (levelup, options) {
  var s = typeof options == 'string' // just an encoding
  if (!s && options && options.encoding && !options.valueEncoding)
    options.valueEncoding = options.encoding
  return extend(
      (levelup && levelup.options) || {}
    , s ? encodingOpts[options] || encodingOpts[defaultOptions.valueEncoding]
        : options
  )
}

function WriteStream (options, db) {
  if (!(this instanceof WriteStream))
    return new WriteStream(options, db)

  Writable.call(this, { objectMode: true })
  this._options = extend(defaultOptions, getOptions(db, options))
  this._db      = db
  this._batch
  this._length  = 0
  this.writable = true
  this.readable = false

  var self = this
  this.on('finish', function f () {
    if (self._batch) {
      return self._flush(f)
    }
    self.writable = false
    self.emit('close')
  })

  this._flusher = function flusher() { self._flush() }
}

inherits(WriteStream, Writable)

WriteStream.prototype._write = function write (d, enc, next) {
  var self = this
    , type = d.type || self._options.type

  if (self._destroyed)
    return
  if (!self._db.isOpen())
    return self._db.once('ready', function () {
      write.call(self, d, enc, next)
    })

  if (!self._batch) {
    self._batch = self._db.batch()
    process.nextTick(self._flusher)
  }

  if (self._options.maxBufferLength &&
      self._length > self._options.maxBufferLength) {
    self._flush()
    write.call(self, d, enc, next)
  }
  else {
    if (type === 'put' || type === 'del') {
      self._batch[type](d.key, d.value, {
          keyEncoding   : d.keyEncoding || self._options.keyEncoding
        , valueEncoding : d.valueEncoding
            || d.encoding
            || self._options.valueEncoding
      })
      self._length++
    }

    next()
  }
}

WriteStream.prototype._flush = function (f) {
  var self   = this
    , batch  = self._batch

  if (self._destroyed || !batch) return
 
  if (!self._db.isOpen()) {
    return self._db.on('ready', function () { self._flush(f) })
  }

  self._length = 0
  self._batch = null

  batch.write(function cb(err) {
    if (err) {
      self.writable = false
      self.emit('error', err)
    }
    else {
      if (f) f()
    }
  })
}

WriteStream.prototype.toString = function () {
  return 'LevelUP.WriteStream'
}

WriteStream.prototype.destroy = function () {
  if (this._destroyed) return
  this._buffer = null
  this._destroyed = true
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

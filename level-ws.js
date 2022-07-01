'use strict'

const Writable = require('readable-stream').Writable

class WriteStream extends Writable {
  constructor (db, options) {
    options = Object.assign({ type: 'put' }, options)

    super({
      objectMode: true,
      highWaterMark: options.highWaterMark || 16,
      autoDestroy: true,
      emitClose: true
    })

    this._options = options
    this._db = db
    this._buffer = []
    this._flushing = false
    this._maxBufferLength = options.maxBufferLength || Infinity
    this._flush = this._flush.bind(this)
  }

  _write (data, enc, next) {
    if (this.destroyed) return

    if (!this._flushing) {
      this._flushing = true
      process.nextTick(this._flush)
    }

    if (this._buffer.length >= this._maxBufferLength) {
      this.once('_flush', (err) => {
        if (err) return this.destroy(err)
        this._write(data, enc, next)
      })
    } else {
      this._buffer.push(Object.assign({ type: this._options.type }, data))
      next()
    }
  }

  _flush () {
    const buffer = this._buffer

    if (this.destroyed) return

    this._buffer = []
    this._db.batch(buffer, (err) => {
      this._flushing = false

      if (!this.emit('_flush', err) && err) {
        // There was no _flush listener.
        this.destroy(err)
      }
    })
  }

  _final (cb) {
    if (this._flushing) {
      // Wait for scheduled or in-progress _flush()
      this.once('_flush', (err) => {
        if (err) return cb(err)

        // There could be additional buffered writes
        this._final(cb)
      })
    } else if (this._buffer && this._buffer.length) {
      this.once('_flush', cb)
      this._flush()
    } else {
      cb()
    }
  }

  _destroy (err, cb) {
    this._buffer = null
    cb(err)
  }
}

module.exports = WriteStream

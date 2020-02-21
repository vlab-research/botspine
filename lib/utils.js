const {PromiseThroughStream} = require('@vlab-research/steez')
const {Writable, Transform} = require('stream')
const retry = require('async-retry')

function _messageWrapper(fn) {
  return async (input) => {

    const message = input.message || input
    const throughData = input.throughData

    let res

    if (throughData) {
      res = await fn(throughData)
    }
    else {
      res = await fn({key: message.key.toString(),
                      value: message.value.toString(),
                      timestamp: message.timestamp.toString()})
    }

    return { message, throughData: res }
  }
}

function _asyncDrain(stream) {
  return new Promise((resolve, _) => {
    stream.once('drain', () => resolve())
  })
}

class ChunkedTransformer extends Transform {
  constructor(fn, n, timeout, opts) {
    super({objectMode: true, ...opts})

    if (!n || n < 2) {
      throw new Error('ChunkedTransformer needs n!')
    }

    this.fn = _messageWrapper(fn)
    this.n = n
    this.chunks = []
    this.timeout = timeout
  }

  async _transform(d, e, c) {
    this.chunks.push(d)

    if (this.chunks.length === this.n) {
      this.processChunks().then(() => c(null)).catch(c)
    }
    else {
      c()

      const timeout = setTimeout(async () => {
        if (this.chunks.length > 0) {
          this.processChunks()
            .catch(err => this.emit('error', err))
        }
        clearTimeout(timeout)
      }, this.timeout)
    }
  }


  processChunks() {

    // prevent race conditions
    const chunks = [...this.chunks]
    this.chunks = []

    return Promise
      .all(chunks.map(this.fn))
      .then(async (results) => {
        for (let r of results) {
          if (!this.push(r)) {
            await _asyncDrain(this)
          }
        }
      })
  }
}

function _retryWrite(ack) {
  const opts = {
    retries: 10,
    onRetry: (err) => {
      console.log('Botspine retrying write due to the error: \n', err)
    }
  }
  return function write({message}, e, c) {
    retry(bail => ack(message), opts).then(_ => c(null)).catch(c)
  }
}

class SpineBase {

  constructor(readable, ack) {
    this._source = readable
    this._sink = new Writable({ objectMode: true, write: _retryWrite(ack) })
  }

  source() { return this._source }
  sink() { return this._sink }

  transform(fn) { return new PromiseThroughStream(_messageWrapper(fn))  }
  chunkedTransform(fn, n, timeout=1000, opts) { return new ChunkedTransformer(fn, n, timeout, opts) }
}


module.exports = { SpineBase }

const should = require('chai').should()
const {SpineBase} = require('./utils')
const {Writable, Readable, pipeline} = require('stream')

class RandomPromiseError extends Error {}


function makeBufferMessage(n) {
  return { key: Buffer.from('foo'), value: Buffer.from(''+n), timestamp: Buffer.from('' + Date.now())}
}

describe('Botspine',  () => {
  it('transforms', (done) => {

    const nums = [2,4]
    const messages = []
    const throughs = []

    const data = nums.map(makeBufferMessage)

    const confirm = () => {
      nums.length.should.equal(messages.length)
      nums.forEach((n,i) => (n*2).should.equal(throughs[i]))
      messages.forEach((m,i) => m.should.equal(data[i]))
      done()
    }

    const source = new Readable({objectMode: true, read: () => null})
    const ack = c => {
      messages.push(c)
      if (nums.length === messages.length) confirm()
    }

    const spine = new SpineBase(source, ack)

    spine.source()
      .pipe(spine.transform(async ({key, value}) => value*2))
      .pipe(spine.transform(async (t) => throughs.push(t)))
      .pipe(spine.sink())

    data.forEach(d => source.push(d))
  })

  it('handles promise rejections on transformers', (done) => {

    const nums = [2,4]
    const data = nums.map(makeBufferMessage)

    const ack = () => null
    const source = new Readable({objectMode: true, read: () => null})

    const spine = new SpineBase(source, ack)


    spine.source()
      .pipe(spine.transform(async ({key, value}) => {
        throw new RandomPromiseError('foo')
      }))
      .on('error', err => {
        err.should.be.instanceof(RandomPromiseError)
        done()
      })
      .pipe(spine.sink())

    data.forEach(d => source.push(d))
  })

  it('emits error events from original stream', (done) => {

    const nums = [2,4]
    const data = nums.map(makeBufferMessage)

    const ack = () => null
    const source = new Readable({objectMode: true, read: () => null})

    const spine = new SpineBase(source, ack)

    spine.source()
      .on('error', err => {
        err.should.be.instanceof(RandomPromiseError)
        done()
      })
      .pipe(spine.sink())

    source.emit('error', new RandomPromiseError('foo'))
  })

  it('retries errors on ack', (done) => {

    const nums = [2,4]
    const messages = []
    const data = nums.map(makeBufferMessage)

    let i = 0

    const ack = (msg) => {
      i++
      if (i < 2) {
        throw new RandomPromiseError('foo')
      }
      else {
        messages.push(msg)
      }

      if (messages.length == nums.length) {
        messages.forEach((m,i) => m.should.equal(data[i]))
        done()
      }
    }

    const source = new Readable({objectMode: true, read: () => null})

    const spine = new SpineBase(source, ack)
    class RandomPromiseError extends Error {}

    spine.source()
      .pipe(spine.transform(async ({key, value}) => value*2))
      .pipe(spine.sink())
      .on('error', err => console.log(err))

    data.forEach(d => source.push(d))
  }).timeout(5000)


  describe('chunkedtransform', () => {

    it('works with timeout if low on data', (done) => {
      const nums = [2,4,6]
      const messages = []
      const data = nums.map(makeBufferMessage)

      const ack = msg => {
        messages.push(msg)

        // check that the first 3 were acked
        if (messages.length === nums.length) {
          messages.forEach((m,i) => m.should.equal(data[i]))
          source.push(data[0])
        }

        // check that a final single message
        if (messages.length > nums.length) {
          messages.length.should.equal(nums.length + 1)
          done()
        }
      }

      const source = new Readable({objectMode: true, read: () => null, highWaterMark: 2})
      const spine = new SpineBase(source, ack)

      spine.source()
        .pipe(spine.chunkedTransform(async ({key, value}) => {
          return value*2
        }, 4, 10))
        .pipe(spine.sink())

      data.forEach(d => source.push(d))
    })

    it('works with saturated chunks of data', (done) => {
      const nums = [2,4,6,8,10,12,14,16]
      const messages = []
      const data = nums.map(makeBufferMessage)

      const ack = msg => {
        messages.push(msg)

        if (messages.length === nums.length) {
          messages.forEach((m,i) => m.should.equal(data[i]))
          source.push(data[0])
        }
        if (messages.length > nums.length) {
          messages.length.should.equal(nums.length + 1)
          done()
        }
      }

      const source = new Readable({objectMode: true, read: () => null})
      const spine = new SpineBase(source, ack)


      spine.source()
        .pipe(spine.chunkedTransform(async ({key, value}) => {
          return value*2
        }, 4, 10))
        .pipe(spine.sink())

      data.forEach(d => source.push(d))
    })


    it('works with multiple transforms', (done) => {
      const nums = [2,4,6,8,10,12,14,16]
      const messages = []
      const values = []
      const data = nums.map(makeBufferMessage)

      const ack = msg => {
        messages.push(msg)

        if (messages.length === nums.length) {
          messages.forEach((m,i) => m.should.equal(data[i]))
          values.forEach((v,i) => v.should.equal(nums[i]*6))
          done()
        }
      }

      const source = new Readable({objectMode: true, read: () => null, highWaterMark: 1})
      const spine = new SpineBase(source, ack)


      spine.source()
        .pipe(spine.chunkedTransform(async (msg) => {
          // get's message if function returns undefined
        }, 200, 10))
        .pipe(spine.chunkedTransform(async ({key, value}) => {
          return value*2
        }, 200, 10))
        .pipe(spine.chunkedTransform(async (value) => {
          return value*3
        }, 200, 10))
        .pipe(spine.chunkedTransform(async (value) => {
          values.push(value)
        }, 200, 10))
        .pipe(spine.sink())

      data.forEach(d => source.push(d))
    })


    it('handles promise rejections on chunked transformers', (done) => {

      const nums = [2,4]
      const data = nums.map(makeBufferMessage)

      const ack = () => null
      const source = new Readable({objectMode: true, read: () => null})
      const spine = new SpineBase(source, ack)

      pipeline(
        spine.source(),
        spine.chunkedTransform(async ({key, value}) => {
          return value*2
        }, 2, 10),
        spine.chunkedTransform(async ({key, value}) => {
          throw new RandomPromiseError('foo')
        }, 2, 10),
        spine.sink(),
        err => {
          err.should.be.instanceof(RandomPromiseError)
          done()
        }
      )

      data.forEach(d => source.push(d))
    })
  })


})

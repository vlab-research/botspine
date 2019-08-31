const should = require('chai').should()
const {SpineBase} = require('./utils')
const {Writable, Readable, pipeline} = require('stream')

class RandomPromiseError extends Error {}

describe('Botspine',  () => {
  it('transforms', (done) => {

    const nums = [2,4]
    const messages = []
    const throughs = []

    const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

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
      .pipe(spine.transformer(async ({key, value}) => value*2))
      .pipe(spine.transformer(async (t) => throughs.push(t)))
      .pipe(spine.sink())

    data.forEach(d => source.push(d))
  })

  it('handles promise rejections on transformers', (done) => {

    const nums = [2,4]
    const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

    const ack = () => null
    const source = new Readable({objectMode: true, read: () => null})

    const spine = new SpineBase(source, ack)


    spine.source()
      .pipe(spine.transformer(async ({key, value}) => {
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
    const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

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
    const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

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
      .pipe(spine.transformer(async ({key, value}) => value*2))
      .pipe(spine.sink())
      .on('error', err => console.log(err))

    data.forEach(d => source.push(d))
  }).timeout(5000)


  describe('chunkedtransformer', () => {

    it('works with timeout if low on data', (done) => {
      const nums = [2,4,6]
      const messages = []
      const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

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
        .pipe(spine.chunkedTransformer(async ({key, value}) => {
          return value*2
        }, 4, 10))
        .pipe(spine.sink())

      data.forEach(d => source.push(d))
    })

    it('works with saturated chunks of data', (done) => {
      const nums = [2,4,6,8,10,12,14,16]
      const messages = []
      const data = nums.map(n => ({ key: Buffer.from('foo'), value: Buffer.from(''+n)}))

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
        .pipe(spine.chunkedTransformer(async ({key, value}) => {
          return value*2
        }, 4, 10))
        .pipe(spine.sink())

      data.forEach(d => source.push(d))
    })
  })


})

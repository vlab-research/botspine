const Kafka = require('node-rdkafka')
const { Writable } = require('stream')
const {PromiseThroughStream} = require('@vlab-research/steez')


class BotSpine {
  constructor(clientId) {
    const brokers = process.env.BOTSPINE_KAFKA_BROKERS
    const topic = process.env.BOTSPINE_MESSAGE_TOPIC

    if (!brokers || !topic) {
      throw new Error(`BotSource cannot be instatiated without brokers and topic!!\n brokers: ${brokers}\n topic: ${topic}`)
    }

    const kafkaOpts = {
      'group.id': clientId,
      'client.id': clientId,
      'enable.auto.commit': false,
      'metadata.broker.list': brokers,
      'retry.backoff.ms': 200,
      'socket.keepalive.enable': true,
      'session.timeout.ms': 60000,
    }

    this.stream = new Kafka.createReadStream(kafkaOpts,
                                             { 'auto.offset.reset': 'earliest' },
                                             { topics: [ topic ]})

    const signals = {
      'SIGHUP': 1,
      'SIGINT': 2,
      'SIGTERM': 15
    }

    for (let [signal, value] in signals) {
      process.on(signal, () => {
        this._disconnect().then(() => process.exit(128 + value))
      })
    }
  }

  source() {
    return this.stream
  }

  transformer(fn) {

    const t = ({message, throughData}) => {
      let res

      if (throughData) {
        res = fn(throughData)
      }
      else {
        res = fn({key: message.key.toString(),
                  value: message.value.toString()})
      }

      return { message, throughData: res }
    }

    return new PromiseThroughStream(t)
  }

  chunkedTransformer(fn, n) {
    // returns a through stream that reads n times, calls the Promise fn on each,
    // and when they all resolve, it passes them one at a time to the next stream
  }

  sink() {
    return new Writable({ objectMode: true,
                          write: ({message},e,c) => {
                            this.stream.consumer.commitMessage(message)
                            c(null)
                          }})
  }

  _disconnect() {
    return new Promise((resolve, reject) =>  {
      this.stream.consumer.on('disconnected', () => {
        resolve()
      })
      this.stream.consumer.disconnect()
    })
  }


}

const Kafka = require('node-rdkafka')
const { Writable } = require('stream')
const { SpineBase } = require('./utils')

class BotSpine extends SpineBase {
  constructor(clientId, streamOpts = {}) {
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

    const stream = new Kafka.createReadStream(kafkaOpts,
                                              { 'auto.offset.reset': 'earliest' },
                                              { topics: [ topic ],
                                                ...streamOpts })

    super(stream, stream.consumer.commitMessage.bind(stream.consumer))
    this.safeShutdown()
  }
}

module.exports = {
  BotSpine
}

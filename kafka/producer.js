/*
const { Kafka, CompressionTypes, logLevel } = require('kafkajs')
const { Partitioners } = require('kafkajs')
6
const kafka = new Kafka({
  logLevel: logLevel.DEBUG,
  brokers: [`localhost:9092`],
  clientId: 'stock-price-producer',
})

const topic = 'update-stock'
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner })

const sendMessage = (req, i) => {
 
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [{key:i, value:req}]
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async (req) => {
  await producer.connect()
  if(req == null || req == undefined) {
   setInterval(() => {
    sendMessage("No messages received", "dummy");
   }, 15000);
  } else {
    sendMessage(req, "valid");
  }

}

run().catch(e => console.error(`[example/producer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`)
      await producer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await producer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

module.exports = run; */
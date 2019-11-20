// Kafka configuration
console.log("000 Kafka base setup start"); //---------------------------------
const kafkaHost = 'apache-kafka:9092';
//const kafkaHost = process.env.KAFKA_HOST
//const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
const JournalRec = require('./models/journalrec')


var KeyedMessage = kafka.KeyedMessage;
const client = new kafka.KafkaClient({kafkaHost: 'apache-kafka:9092'});

console.log("200 COnsumer part");//---------------------------------		
 var Consumer = kafka.Consumer,
     consumer = new Consumer(
        client,
        [
            { topic: 'warehouse-movement', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );	
	

console.log("210 COnsumer part - nactu");//---------------------------------	
consumer.on('message', function (message) {
    console.log(message);
});	



/*
try {
    console.log('Kafka Consumer is booting up ... (kafkaHost:'+kafkaHost + ' kafkaTopic:' + kafkaTopic + ')')

    const client = new kafka.KafkaClient(kafkaHost)
    const topics = [
        {
            topic: kafkaTopic, 
            partition: 0
        }
    ]
    const options = {
        autoCommit: true, 
        fetchMaxWaitMs: 1000, 
        fetchMaxBytes: 1024 * 1024, 
        encoding: 'utf8', 
        fromOffset: false
    }
    
    const consumer = new kafka.Consumer(client, topics, options)

    consumer.on('message', async (message) => {    
        const journalrec = new JournalRec(JSON.parse(message.value))
        try {
            await journalrec.save()
            console.log('Journal record saved successfully')
        }catch(e) {
            console.log(e)
        }
    })

    consumer.on('error', (err) => {
        console.log('error', err)
    })
}catch(e) {
    console.log(e)
}
*/
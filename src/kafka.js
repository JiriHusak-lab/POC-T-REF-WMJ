const kafkaHost = process.env.KAFKA_HOST
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
const JournalRec = require('./models/journalrec')

try {
    console.log('Kafka Consumer is booting up ... (kafkaHost:'+kafkaHost + ' kafkaTopic:' + kafkaTopic + ')')
    //const client = new kafka.KafkaClient(kafkaHost)
	const client = new kafka.KafkaClient({kafkaHost: 'apache-kafka:9092'});
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
    
    console.log("200 Consumer1 part JH");//---------------------------------		
	var Consumer = kafka.Consumer,
     consumer1 = new Consumer(
        client,
        [
            { topic: 'warehouse-movement', partition: 0 }
        ],
        {
            autoCommit: false
        }
    );
	console.log("210 Consumer1 part - nactu");//---------------------------------
	consumer1.on('message', function (message) {
		console.log(message);
    });	

		
	const consumer = new kafka.Consumer(client, topics, options)
    consumer.on('message', async (message) => {    
        const journalrec = new JournalRec(JSON.parse(message.value))
        try {
            await journalrec.save()
            console.log('Journal record saved successfully')
        }catch(e) {
            console.log('Consumer.on e:' + e)
        }
    })

    consumer.on('error', (err) => {
        console.log('Consumer.on error' + err)
    })
	
	

}catch(e) {
    console.log(e)
}

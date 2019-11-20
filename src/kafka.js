const kafkaHost = process.env.KAFKA_HOST
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
const JournalRec = require('./models/journalrec')

try {
    console.log('Kafka Consumer is booting up ... (kafkaHost:'+kafkaHost + ' kafkaTopic:' + kafkaTopic + ')')
    //const client = new kafka.KafkaClient(kafkaHost)
	console.log("100 Consumer1 part - open JN");//---------------------------------		
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
    	
	const consumer = new kafka.Consumer(client, topics, options)
	
	console.log("110 Consumer1 part - nactu JN");//---------------------------------
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
	
    console.log("200 Consumer1 part - open JH");//---------------------------------		
	var Consumer = kafka.Consumer,
     consumer1 = new Consumer(
        client,
        [
            { topic: 'warehouse-movement', partition: 0 }
        ],
        {
            autoCommit: true
        }
    );
	console.log("210 Consumer1 part - nactu JH");//---------------------------------
	consumer1.on('message', function (message) {
		console.log(message);
    });		

}catch(e) {
    console.log(e)
}

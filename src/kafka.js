const kafka = require('kafka-node')
const kafkaHost = process.env.KAFKA_HOST
const kafkaPort = process.env.KAFKA_PORT
const kafkaHostEnv = process.env.KAFKA_HOST_ENV
const kafkaTopic = process.env.KAFKA_TOPIC
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;

const JournalRec = require('./models/journalrec')

let mDate = new Date();
let mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');

try {
    console.log(mDateStr + ': Kafka Consumer is booting up ... (ENVs: kafkaHost:\"' + kafkaHost + '\"; kafkaPort:' + kafkaPort + '; kafkaTopic:' + kafkaTopic + '; kafkaHostEnv:' + kafkaHostEnv + '; )');
    
    const client = new kafka.KafkaClient({kafkaHost: kafkaHost + ':' + kafkaPort});
    
    const topics = [
        {
            topic: kafkaTopic, 
            partition: 0
        }
    ]
    const options = {
		//groupID: 'wmj-journal',         //consumer group id
        autoCommit: false, 
		autoCommitIntervalsMs: 5000,
        fetchMaxWaitMs: 1000, 
		fetchMinBytes: 1,
        fetchMaxBytes: 1024 * 1024,
		fromOffset: true,
        encoding: 'utf8', 
        fromOffset: false
    }
    
    const consumer = new Consumer(client, topics, options)
	var offset = new Offset(client)

    consumer.on('message', async (message) => {  
		console.log(mDateStr + ': consumer.on - listening: ')
		// a zobrazit offset
		offset.fetch([
			{ topic: 'warehouse-movement' }
			], function (err, data) {
					console.log(mDateStr + ': Offset fetch:' + data);
			})
			
        const journalrec = new JournalRec(JSON.parse(message.value))
        try {
            await journalrec.save()
            mDate = new Date();
            mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
            console.log(mDateStr + ': Journal record saved successfully: ' + message.value)
	
			// tady by mel byt commit;
			console.log(mDateStr + ': Bude commit:');
			consumer.commit(function(err, data) {
			});
			
			// a zobrazit offset
			offset.fetch([
				{ topic: 'warehouse-movement' }
			], function (err, data) {
				console.log(mDateStr + ': Offset fetch:' + data);
			});
			
        }catch(e) {
            mDate = new Date();
            mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
            console.log(mDateStr + ': journalrec.save() error:' + e)
        }
    })

    consumer.on('error', (err) => {
        mDate = new Date();
        mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
        console.log(mDateStr + ': Consumer on error' + err)
    })
}catch(e) {
    console.log(e)
}

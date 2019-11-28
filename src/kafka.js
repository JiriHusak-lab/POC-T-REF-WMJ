const kafkaHost = process.env.KAFKA_HOST
const kafkaHostEnv = process.env.KAFKA_HOST_ENV
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
const JournalRec = require('./models/journalrec')

var mDate = new Date();
var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');

try {
    console.log(mDateStr + ': Kafka Consumer is booting up ... (ENVs: kafkaHost:'+kafkaHost + '; kafkaTopic:' + kafkaTopic + '; kafkaHostEnv:' + kafkaHostEnv + '; )')
    //const client = new kafka.KafkaClient(kafkaHost)
    const client = new kafka.KafkaClient({kafkaHost: kafkaHostEnv + ':9092'});
    //const client = new kafka.KafkaClient({kafkaHost: 'apache-kafka:9092'});
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

	
	console.log(mDateStr + ': 100 Kafka PRODUCER part start');//---------------------------------
	var Producer = kafka.Producer;
	var KeyedMessage = kafka.KeyedMessage;
	
	var mDate = new Date();
	var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
	var mMatid = mDate.toString('h:mm');
	//var jsonmsg = {  "mnozstvi": 8,  "mvm1": "wh1",  "mvm2": "wh2",  "kmat": "mat"+mMatid+"1",  "hmotnost": 12}
	
	var	producer = new Producer(client),
		km = new KeyedMessage('key', 'message'),
		payloads = [
		//{ topic: 'warehouse-movement', messages: '{"mnozstvi":8,"mvm1":"wh1","mvm2":"wh2","kmat":"mat001","hmotnost":12}', partition: 0 }
		{ topic: 'warehouse-movement', messages: 'PEPIK', partition: 0 }
		];
	console.log(mDateStr + ': 110 PRED Kafka PRODUCER.on')
	producer.on('ready', function () {
		producer.send(payloads, function (err, data) {
			console.log(data);
			console.log(mDateStr + ': 009 Producer.on ready');
		});
		console.log("140 Producer.on ready");
	});
	console.log("200 Kafka PRODUCER part END");//---------------------------------
	
	
	
	// Consumer PART --------------------------------------------
	/*
    const consumer = new kafka.Consumer(client, topics, options)

    consumer.on('message', async (message) => {    
        const journalrec = new JournalRec(JSON.parse(message.value))
        try {
            await journalrec.save()
            mDate = new Date();
            var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
            console.log(mDateStr + ': Journal record saved successfully: ' + message.value)
        }catch(e) {
            mDate = new Date();
            var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
            console.log(mDateStr + ': journalrec.save() error:' + e)
        }
    })

    consumer.on('error', (err) => {
        mDate = new Date();
        var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
        console.log(mDateStr + ': Consumer on error' + err)
    })
	*/
	// END Consumer PART --------------------------------------------
	
}catch(e) {
    console.log(e)
}

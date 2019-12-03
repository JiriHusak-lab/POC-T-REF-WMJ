const kafkaHost = process.env.KAFKA_HOST
const kafkaHostEnv = process.env.KAFKA_HOST_ENV
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
const JournalRec = require('./models/journalrec')
const podIP = process.env.MY_POD_IP

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
	const moment = require('moment');
	m = moment().format('[The time is] h:mm:ss a'); 
	//var mMatid = mDate.toString('ddhmm');
	var mMatid = podIP + '-' + moment().format('DDhhmm'); 
	console.log(mDateStr + ': 103 Kafka PRODUCER; mMatid:' + mMatid);
	var jsonmsga = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}'
	var jsonmsgb = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'b\",\"hmotnost\":12}'
	var jsonmsgc = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'c\",\"hmotnost\":12}'
	var jsonmsgd = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'd\",\"hmotnost\":12}'
	var jsonmsge = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'e\",\"hmotnost\":12}'
	var jsonmsgf = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'f\",\"hmotnost\":12}'
	var jsonmsgg = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'g\",\"hmotnost\":12}'
	var jsonmsgh = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'h\",\"hmotnost\":12}'
	var jsonmsgi = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'i\",\"hmotnost\":12}'
	var jsonmsgj = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'j\",\"hmotnost\":12}'
	
	var	producer = new Producer(client),
		km = new KeyedMessage('key', 'message'),
		payloads = [
		//{ topic: 'warehouse-movement', messages: '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}', partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsga, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgb, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgc, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgd, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsge, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgf, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgg, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgh, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgi, partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsgj, partition: 0 }
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
	
	
	console.log(mDateStr + ': 300 Kafka PRODUCER WHILE part start');//---------------------------------
	while (true) {
		// execute code as long as condition is true
		var mMatid = podIP + '-' + moment().format('DDhhmm'); 
		var jsonmsgX = '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}'
		
		var	producer = new Producer(client),
		km = new KeyedMessage('key', 'message'),
		payloads = [
				{ topic: 'warehouse-movement', messages: jsonmsgX, partition: 0 }
		];
		console.log(mDateStr + ': 310 PRED Kafka PRODUCER.on WHILE')
		producer.on('ready', function () {
			producer.send(payloads, function (err, data) {
				console.log(data);
				console.log(mDateStr + ': 309 Producer.on ready');
			});
			console.log("340 Producer.on ready");
		});
		
        producer.on('error', (err) => {
			mDate = new Date();
			var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
			console.log(mDateStr + ': Producer WHILE on error' + err)
		})		
		
		//setTimeout(function(){
		//	i++;
		//}, 3000);
		
		sleep(60000, function() {
			// executes after one second, and blocks the thread
		});
	}
	console.log("400 Kafka PRODUCER WHILE part END");//---------------------------------
	
	
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

function sleep(time, callback) {
    var stop = new Date().getTime();
    while(new Date().getTime() < stop + time) {
        ;
    }
    callback();
}

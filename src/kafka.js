const kafkaHostEnv = process.env.KAFKA_HOST
const kafkaPort = process.env.KAFKA_PORT
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
//const pafka = require('pafka-node')
const podIP = process.env.MY_POD_IP

var mDate = new Date();
var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');

//try {
    console.log(mDateStr + ': Ver20191203:21:41 Kafka Consumer is booting up ... (ENVs: kafkaHost:\"' + kafkaHostEnv + '\"; kafkaPort:' + kafkaPort + '; kafkaTopic:' + kafkaTopic + '; kafkaHostEnv:' + kafkaHostEnv + '; )');
	
	var options = {};

	options.requireAcks = 1
	options.ackTimeoutMs = 100
	options.partitionerType = 1

	const Producer = kafka.Producer;
    //const client = new kafka.KafkaClient({kafkaHost: 'apache-safka:1111'});
	const client = new kafka.KafkaClient({kafkaHost: kafkaHostEnv + ':'+ kafkaPort});
	client.on('ready', function (){
        console.log('client ready event');
    }) 
	client.on('error', function (err) {
        console.log('client error event: ', err);
    });
	
	var client2 = new kafka.Client('apache-kafka:9092');
	client2.on('ready', function (){
        console.log('client ready event');
    }) 
	client2.on('error', function (err) {
        console.log('client error event: ', err);
    });
	if (client2.ready) {
		console.log('client is ready');
	}
	else {
		console.log('client is not ready');
	}

	const client3 = new kafka.KafkaClient({kafkaHost:'apache-kafka:9092'});
	client3.on('ready', function (){
        console.log('client ready event');
    }) 
	client3.on('error', function (err) {
        console.log('client error event: ', err);
    });
	

	producer = new Producer(client, options);
	if (producer.ready) {
		console.log('producer is ready');
	}
	else {
		console.log('producer is not ready');
	}
	


	producer.on('ready', function(err, response) {
            console.log(mDateStr + ': Kafka Producer is Ready to communicate with Kafka on: ' + kafkaHostEnv + ':' + kafkaPort);
			
			client.refreshMetadata(["test"], function(err3) {
				if (!err3) {
                        producer.send(payload, function(err2, result) {
                                  producer.close();
                                  client.close();
                        })
				}
			})
	})
	
	console.log(mDateStr + ': 100 Kafka PRODUCER part start');//---------------------------------
	var mDate = new Date();
	var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
	const moment = require('moment');
	var mMatid = podIP + '-' + moment().format('DDhhmm'); 
	console.log(mDateStr + ': 103 Kafka PRODUCER; mMatid:' + mMatid);
	var jsonmsga = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}';
	let payload = [{ topic: 'warehouse-movement', messages: jsonmsga, partition: 0 }];
	
	
	console.log(mDateStr + ': 110 PRED Kafka PRODUCER.on');
	producer.on('ready', async function () {
		console.log(mDateStr + ': 111 Kafka PRODUCER.on PRED PRODUCER.SEND');
		producer.send(payload, function (err, data) {
			console.log(data);
			console.log(mDateStr + ': 009 Producer.on ready sent');
		});
		console.log("140 Producer.on ready");
	});
	
    producer.on('error', (err) => {
		mDate = new Date();
		var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
		console.log(mDateStr + '190: Producer on error' + err);
	})	
	console.log("200 Kafka PRODUCER part END");//---------------------------------


	producer.send(payload, function (err, data) {
		console.log(mDateStr + ':Sent data:' + data);
		console.log(mDateStr + ': 309 Producer.on ready sent');
	});	
	
	
	console.log(mDateStr + ': 300 Kafka PRODUCER WHILE part start');//---------------------------------
	while (true) {
		// execute code as long as condition is true
		var mMatid = podIP + '-' + moment().format('DDhhmm'); 
		var jsonmsgX = '{\"mnozstvi\":8,\"mvm3\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}'
		console.log(mDateStr + ': 308 jsonmsgX:' + jsonmsgX);
		
		payload = [
				{ topic: 'warehouse-movement', messages: jsonmsgX, partition: 0 }
		];

			producer.send(payload, function (err, data) {
				console.log(mDateStr + ':Sent data:' + data);
				console.log(mDateStr + ': 309 Producer.on ready');
			});
			
			
			

		var mDate = new Date();
		var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
		console.log(mDateStr + ': 310 PRED Kafka PRODUCER.on WHILE');

		producer.on('ready', function () {
			producer.send(payloads, function (err, data) {
				console.log(mDateStr + ':Sent data:' + data);
				console.log(mDateStr + ': 309 Producer.on ready');
			});
			console.log(mDateStr + ':340 Producer.on ready');
		});

        producer.on('error', (err) => {
			mDate = new Date();
			var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
			console.log(mDateStr + ': Producer WHILE on error' + err);
		})		
		
		console.log(mDateStr + ': 311 PRED Kafka PRODUCER.on WHILE  SLEEP');
		sleep(60000, function() {
		});
		var mDate = new Date();
		var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');	
		console.log(mDateStr + ': 311 PO Kafka PRODUCER.on WHILE  SLEEP');
	};
	console.log("400 Kafka PRODUCER WHILE part END");//---------------------------------
	

	
//}catch(e) {
//    console.log(mDateStr + ': HLAVNI Catch' + e)
//}

function sleep(time, callback) {
    var stop = new Date().getTime();
    while(new Date().getTime() < stop + time) {
        ;
    }
    callback();
}
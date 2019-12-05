//const kafkaHostEnv = process.env.KAFKA_HOST
const kafkaPort = process.env.KAFKA_PORT
const kafkaHostEnv = process.env.KAFKA_HOST_ENV
const kafkaTopic = process.env.KAFKA_TOPIC
const kafka = require('kafka-node')
//const pafka = require('pafka-node')
const podIP = process.env.MY_POD_IP

var mDate = new Date();
var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');

//try {
    console.log(mDateStr + ': Ver20191203:21:41 Kafka Consumer is booting up ... (ENVs: kafkaHostEnv:\"' + kafkaHostEnv + '\"; kafkaPort:' + kafkaPort + '; kafkaTopic:' + kafkaTopic + '; kafkaHostEnv:' + kafkaHostEnv + '; )');
	
	var options = {};

	options.requireAcks = 1
	options.ackTimeoutMs = 100
	options.partitionerType = 1

	const Producer = kafka.Producer;
    const client = new kafka.KafkaClient({kafkaHost: 'apache-safka:1111'});
	//const client = new kafka.KafkaClient({kafkaHost: kafkaHostEnv + ':'+ kafkaPort});
	producer = new Producer(client, options);
	if (client.ready) {
		console.log('client is ready');
	}
	else {
		console.log('client is not ready');
	}

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
	
	console.log(mDateStr + ': 100 Kafka PRODUCER part start');//---------------------------------
	var mDate = new Date();
	var mDateStr = mDate.toString('dddd MMM yyyy h:mm:ss');
	const moment = require('moment');
	var mMatid = podIP + '-' + moment().format('DDhhmm'); 
	console.log(mDateStr + ': 103 Kafka PRODUCER; mMatid:' + mMatid);
	var jsonmsga = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}';
	var jsonmsgb = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'b\",\"hmotnost\":12}';
	var jsonmsgc = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'c\",\"hmotnost\":12}';
	var jsonmsgd = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'd\",\"hmotnost\":12}';
	var jsonmsge = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'e\",\"hmotnost\":12}';
	var jsonmsgf = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'f\",\"hmotnost\":12}';
	var jsonmsgg = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'g\",\"hmotnost\":12}';
	var jsonmsgh = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'h\",\"hmotnost\":12}';
	var jsonmsgi = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'i\",\"hmotnost\":12}';
	var jsonmsgj = '{\"mnozstvi\":8,\"mvm2\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'j\",\"hmotnost\":12}';

	let payload = [
		//{ topic: 'warehouse-movement', messages: '{\"mnozstvi\":8,\"mvm1\":\"wh1\",\"mvm2\":\"wh2\",\"kmat\":\"mat'+ mMatid + 'a\",\"hmotnost\":12}', partition: 0 },
		{ topic: 'warehouse-movement', messages: jsonmsga, partition: 0 }
		//{ topic: 'warehouse-movement', messages: jsonmsgb, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgc, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgd, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsge, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgf, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgg, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgh, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgi, partition: 0 },
		//{ topic: 'warehouse-movement', messages: jsonmsgj, partition: 0 }
		];
	
	
	
	
	
	
	
		console.log(mDateStr + ':  010 PETR PRED Kafka PRODUCER.on');
    console.log('Going to use producer ..');
    try {
        // Kafka Producer Configuration 
        console.log('Trying to connect to Kafka server: ' + kafkaHostEnv + ':' + kafkaPort + ', topic: ' + kafkaTopic);
        const Producer = kafka.Producer;
        const client = new kafka.KafkaClient({kafkaHost: kafkaHostEnv + ':'+ kafkaPort});
        const producer = new Producer(client);
    
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
			else ()
	   
	   
            let push_status = producer.send(payload, function (err, data) {
                if (err) {
                    console.log('Broker update failed: ' + err);
                    cb(err);
                } else {
                    console.log('Broker update success: ' + data);
                    cb(resp,null);
                }
            });
        })
    
        producer.on('error', function(err) {
            console.log(err);
            console.log(mDateStr + ': [kafka-producer -> '+kafkaTopic+']: connection errored');
            throw err;
        })
    }
        catch(e) {
        console.log(mDateStr + ': ' + e);
    }
		console.log(mDateStr + ': 020 PETR PO Kafka PRODUCER.on');
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	console.log(mDateStr + ': 110 PRED Kafka PRODUCER.on');
	producer.on('ready', async function () {
		console.log(mDateStr + ': 111 PRED Kafka PRODUCER.SEND');
		producer.send(payload, function (err, data) {
			console.log(data);
			console.log(mDateStr + ': 009 Producer.on ready');
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
				console.log(mDateStr + ': 309 Producer.on ready');
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

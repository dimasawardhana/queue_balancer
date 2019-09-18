var amqp = require('amqplib/callback_api');
let fs = require("fs");
var csv = require('csv-parser');
var setting = require('./settings');
var argv = require('minimist')(process.argv.slice(2));

if(!argv.number){
    console.log("please get argument number to pass");
    console.log("number is number of data to Process, based on data available");
    process.exit();
}else if(!setting.list.includes(argv.number)){
    console.log("number isn't available to process");
    process.exit();
}

if(!argv.queues){
    console.log("please get number of queues to process using --queues=<number>");
    console.log("available number for now up to "+setting.queue_name.length);
    process.exit();
}
var csvpath = setting.csvPath+"data_"+argv.number+".csv";
var host = setting.hostAmqp;
let queuesNo = argv.queues;
let container = []
count = 0;
fs.createReadStream(csvpath)
        .pipe(csv())
        .on('data', (data)=>{
            data["toQueue"] = (count % queuesNo);
            count++;
            container.push(data);
        })
        .on("end", ()=>{
            amqp.connect(host, (err, connection)=>{
                if(err){
                    throw err;
                }
                connection.createChannel((error,channel)=>{
                    if(error){
                        throw error;
                    }
                    var queues = [...setting.queue_name];// nama queuenya apa.
                    for(queue of queues){
                        channel.assertQueue(queue,{
                            durable:true // to let you know that queue isn't durable when it's off the message will be erased
                        });
                    }
                    for(data in container)    {
                        let toSend = JSON.stringify(container[data]);
                        channel.sendToQueue(queues[container[data].toQueue],Buffer.from(toSend));
                        console.log("[x] Sent %s to %s", container[data].id, queues[container[data].toQueue]);
                    }
                });
                setTimeout(function() {
                    connection.close();
                    process.exit(0);
                }, 500);
            })
        })
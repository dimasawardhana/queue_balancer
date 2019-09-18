var amqp = require('amqplib/callback_api');
let fs = require("fs");
var csvpath = "./dataset/data_4000.csv";
var csv = require('csv-parser');
var host = 'amqp://localhost';
let queues = 3;
let container = []
count = 0;
fs.createReadStream(csvpath)
        .pipe(csv())
        .on('data', (data)=>{
            data["toQueue"] = (count % queues) + 1;
            count++;
            container.push(data);
        })
        .on("end", ()=>{
            console.log("oyoyoyoy")
            amqp.connect(host, (err, connection)=>{
                if(err){
                    throw err;
                }
                connection.createChannel((error,channel)=>{
                    if(error){
                        throw error;
                    }
                    var queue = 'queueueu';// nama queuenya apa.
                    channel.assertQueue(queue,{
                        durable:true // to let you know that queue isn't durable when it's off the message will be erased
                    });
                    
                    
                    for(data in container)    {
                        let toSend = JSON.stringify(container[data]);
                        channel.sendToQueue(queue,Buffer.from(toSend));
                        console.log("[x] Sent %s", container[data].id);
                    }
                });
                setTimeout(function() {
                    connection.close();
                    process.exit(0);
                }, 500);
            })
        })

var ws = require("nodejs-websocket")
var kafka = require('kafka-node')

var Consumer = kafka.Consumer;
// Scream server example: "hi" -> "HI!!!" 
var server = ws.createServer(function (conn) {
	console.log("New connection")

        var client = new kafka.Client();
        console.log(client);
        var consumer = new Consumer(
          client,
          [
            { topic: 'mikeg', partition: 0}, { topic: 'mikeg', partition: 1}, { topic: 'mikeg', partition: 2},{ topic: 'mikeg', partition: 3} 
          ],
          {
            autoCommit: false
          });
        console.log(consumer);
        consumer.on('message', function(message) {
          conn.sendText(message.value);
        });
        consumer.on('error', function(err) {
          console.log(err);
        });
	conn.on("text", function (str) {
		console.log("Received "+str)
		conn.sendText(str.toUpperCase()+"!!!")
	});
        conn.on("error", function(err) {
          console.log("ERROR: " + err)
        });
	conn.on("close", function (code, reason) {
		console.log("Connection closed")
	})
}).listen(8001)

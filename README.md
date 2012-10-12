A node.js port of pynsq reader from https://github.com/bitly/nsq

Very much untested and unfinished. Basic reading/publishing works.


npm install nsq


Reader example:

var NSQReader = require('../lib/nsqreader.js').Reader;

var reader = new NSQReader({
  task1:function(body){console.log('Task handled:');console.log(body)}}, 'test', 'ch1', {lookupd_http_addresses:['127.0.0.1:4161'], data_method:function (conn, task, message) {
  console.log(message);
}})
reader.run();


Publishing example:

var NSQ = require('../lib/nsq.js');
CONN = require("../lib/conn.js");

var conn = CONN.createClient();
conn.connect();

conn.on("connect", function () {
  conn.send("  V2");
  var cmd = NSQ.publish('test', "hello ");
  conn.send(cmd);
  
});


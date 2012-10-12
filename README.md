[![build status](https://secure.travis-ci.org/phillro/nodensq.png)](http://travis-ci.org/phillro/nodensq)

A node.js port of pynsq reader from https://github.com/bitly/nsq

Very much untested and unfinished. Basic reading/publishing works.


Installation
--------------

npm install nsq


Reader example
----------------

var NSQReader = require('../lib/nsqreader.js').Reader;

var taskFunction = function(message){
    console.log(message)
}

var reader = new NSQReader({"task1":taskFunction},
  'topic',
  'channel',
  {lookupd_http_addresses:['127.0.0.1:4161']}
)
reader.run();



Publishing example
--------------------

var NSQ = require('../lib/nsq.js');
CONN = require("../lib/conn.js");

var conn = CONN.createClient();
conn.connect();

conn.on("connect", function () {
  conn.send("  V2");
  var cmd = NSQ.publish('test', "hello ");
  conn.send(cmd);  
});


/**
 * User: philliprosen
 * Date: 10/9/12
 * Time: 4:59 PM
 */

var extend = require('node.extend'),
  http = require('http'),
  BackOffTimer = require('./backofftimer').BackOffTimer,
  NSQ = require('./nsq');
Conn = require('./conn');

function Reader(all_tasks, topic, channel, params) {
  this.all_tasks = all_tasks;
  this.topic = topic;
  this.channel = channel;
  this.backoff_timer = {};
  this.conns = {};
  this.connectionCount = 0;

  this.params = extend({
    host:'127.0.0.1',
    port:4150,
    nsqd_tcp_addresses:[],
    lookupd_http_addresses:[],
    max_tries:5,
    max_in_flight:1,
    requeue_delay:90,
    lookupd_poll_interval:120,
    preprocess_method:false,
    validate_method:false,
    data_method:false,
    connect_method:false,
  }, params)

  this.requeue_delay = this.params.requeue_delay * 1000;
  this.task_lookup = all_tasks


  for (var k in this.task_lookup) {
    this.backoff_timer[k] = new BackOffTimer(0, 120);
  }

  console.log('starting reader for topic ' + this.topic);

  for (var t in this.task_lookup) {
    var task = this.task_lookup[t];
    for (a in this.params.nsqd_tcp_addresses) {
      var address = this.params.nsqd_tcp_addresses[a].split(':')[0];
      var port = this.params.nsqd_tcp_addresses[a].split(':')[1];
      this.connectToNsqd(address, port, task);
    }
  }
  this.queryLookupd()
}

Reader.prototype.data = function (conn, raw_data, task) {
  var self = this;
  var unpackedResponse = nsq.unpack_response(raw_data);
  var frame = unpackedResponse[0];
  var data = unpackedResponse[1];
  if (frame == NSQ.FRAME_TYPE_MESSAGE) {

    message = NSQ.decode_message(data)
    this.handleMessage(conn, task, message, function (err, conn, task, message) {
      if(err){
        console.log('Error handling message '+err);
      }
      if(!err&&typeof self.params.data_method=='function'){
        self.params.data_method(conn, task, message);
      }
    });

  } else if (frame == NSQ.FRAME_TYPE_MESSAGE && data == "__heartbeat_") {
    this.updateHeartbeat()
    conn.send(NSQ.nop())
  }

}

Reader.prototype.close = function (conn,task) {
  this.conns[conn.connId] = null;
  this.connectionCount--;

  if(this.connectionCount==0){
    console.log('All connections closed. Exiting')
    process.exit(0)
  }
}

Reader.prototype.handleMessage = function (conn, task, message, cb) {
  conn.ready -= 1

  // update ready count if necessary...
  // if we're in a backoff state for this task
  // set a timer to actually send the ready update
  var per_conn = this.connection_max_in_flight();
  if (!conn.is_sending_ready && (conn.ready <= 1 || conn.ready < (per_conn * .025))) {
    var backoff_interval = this.backoff_timer[task];
    if (backoff_interval > 0) {
      conn.is_sending_ready = true;
      console.log(conn + ' backing off for ' + backoff_interval + ' seconds');
      setTimeout(function () {
        this.sendReady(conn, per_conn);
      }, backoff_interval * 1000);
    } else {
      this.sendReady(conn, per_conn);
    }
  }
  try {
    message.body = JSON.parse(message.body);
    cb(undefined, conn, task, message);
  } catch (ex) {
    cb(ex);
  }

}

Reader.prototype.sendReady = function (conn, value) {
  conn.send(nsq.ready(value))
  conn.ready = value
  conn.is_sending_ready = false
}

Reader.prototype.requeue = function (conn, message, delay) {
  var delay = delay || true;
  if (this.message.attempts > this.max_tries) {
    console.log(conn + ' giving up on message after max tries ' + message.body)
    return this.finish(conn, message.id)
  } else {
    requeue_delay = delay ? this.requeue_delay * message.attempts : 0;
    conn.send(nsq.requeue(message._id, requeue_delay))
  }
}

Reader.prototype.finish = function (conn, message_id) {
  conn.send(NSQ.finish(message_id));
}

Reader.prototype.connectionMaxInFlight = function () {
  return (1 > this.max_in_flight ? 1 : this.max_in_flight) / (1 > this.connectionCount ? 1 : this.connectionCount)
}

Reader.prototype.updateHeartbeat = function () {
  console.log('Heart beat.')
}

Reader.prototype.connectToNsqd = function (address, port, task) {

  var conn_id = address + ':' + str(port) + ':' + task;
  if (this.conns[conn_id] === -1) {
    console.log(address + ':' + port + " connecting to nsqd for " + task);
    this.connectionCount++;
    this.conns[conn_id] = Conn.createClient(address, port, task);
    if (typeof this.params.connect_method == 'function') {
      this.conns[conn_id].on('connect', this.params.connect_method);
    }
    this.conns[conn_id].on('data', this.data);
    this.conns[conn_id].on('close', this.close);
    this.conns[conn_id].connect();
  }
}

Reader.prototype.queryLookupd = function () {
  var self = this;
  for (var e in this.params.lookupd_http_addresses) {
    var url = [this.params.lookupd_http_addresses[e], "/lookup?topic=", encodeURI(this.topic)].join();


    var options = {
      host: this.params.host,
      port: this.params.port,
      path: "/lookup?topic="+encodeURI(this.topic),
      method: 'GET'
    };
    //var req = this.http_client.request('get', url);
    var req = http.request(options, function (res) {
      var data = '';
      if (response.statusCode != 200) {
        console.log('queryLookupd error: ' + response);
      } else {
        res.on('data', function (chunk) {
          data += chunk;
        })
        res.on('end', function () {
          data = JSON.parse(data)
          self.finishQueryLookupd(data);
        })
      }
    })
    req.on('error', function(e) {
      console.log('problem with request: ' + e.message);
    });


    req.end();
  }
}

Reader.prototype.finishQueryLookupd = function (lookupData) {
  for (var t in this.task_lookup) {
    var task = this.task_lookup[t];
    for (var p in lookupData['data']['producers']) {
      var producer = lookupData['data']['producers'][p];
      this.connectToNsqd(producer['address'], producer['tcp_port'], task);
    }
  }
}

exports.Reader = Reader;
/**
 * User: philliprosen
 * Date: 10/9/12
 * Time: 4:59 PM
 */

var extend = require('node.extend'),
  http = require('http'),
  NSQ = require('./nsq'),
  Conn = require('./conn'),
  BackOffTimer = require('./backofftimer').BackOffTimer;

function Reader(all_tasks, topic, channel, params) {
  var self = this;
  this.task_lookup = all_tasks;
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
    max_in_flight:10,
    requeue_delay:90,
    lookupd_poll_interval:120,
    preprocess_method:false,
    validate_method:false,
    data_method:false,
    connect_method:false,
  }, params)

  this.requeue_delay = this.params.requeue_delay * 1000;
  this.task_count = 0;
  for (var t in this.task_lookup) {
    this.task_count++;
  }

  for (var k in this.task_lookup) {
    this.backoff_timer[k] = new BackOffTimer(0, 120);
  }

  console.log("Starting reader for topic " + this.topic);

  for (var t in this.task_lookup) {
    var task = this.task_lookup[t];
    for (a in this.params.nsqd_tcp_addresses) {
      var address = this.params.nsqd_tcp_addresses[a].split(':')[0];
      var port = this.params.nsqd_tcp_addresses[a].split(':')[1];
      this.connectToNsqd(address, port, task);
    }
  }
  this.short_hostname = this.params.host.split('.')[0];
  this.hostname = this.params.host;
  this.queryLookupd()

  //polls querylookupd for new services
  this.poll = function () {
    var poll_interval = self.params.lookupd_poll_interval;
    setTimeout(function () {
      self.queryLookupd(self.poll);
    }, poll_interval * 1000)
  }

  this.connect = function (conn, task) {
    if (self.task_count > 1) {
      var channel = self.channel + '.' + task;
    } else {
      channel = self.channel;
    }
    var initial_ready = self.connectionMaxInFlight();

    conn.send("  V2");
    conn.send(NSQ.subscribe(self.topic, channel, self.short_name, self.hostname));
    //conn.send(NSQ.subscribe('test', 'ch1', self.short_name, self.hostname));
    //var subCmd = NSQ.subscribe("test", "nsq_to_file", "a", "b");
    //conn.send(subCmd);
    conn.send(NSQ.ready(initial_ready));
    conn.ready = initial_ready;
    conn.is_sending_ready = false;
  }

  this.close = function (conn, task) {
    self.conns[conn.connId] = null;
    self.connectionCount--;
    if (this.connectionCount == 0) {
      console.log('All connections closed. Exiting')
      process.exit(0)
    }
  }

  this.finish = function (conn, message_id) {
    conn.send(NSQ.finish(message_id));
  }

  this.data = function (conn, raw_data, task) {
    var unpackedResponse = NSQ.unpack_response(raw_data);
    var frame = unpackedResponse[0];
    var data = unpackedResponse[1];
    if (frame == NSQ.FRAME_TYPE_MESSAGE) {
      var message = NSQ.decode_message(data)
      self.handleMessage(conn, task, message, function (err, conn, task, message) {
        var body = message.body;
        if (typeof self.params.preprocess_method == 'function') {
          body = self.params.preprocess_method(body);
        }
        if (typeof self.params.validate_method == 'function' && !self.params.validate_method(body)) {
          return self.finish(conn, message.id);
        }

        if (typeof self.task_lookup[task] == 'function') {
          self.task_lookup[task](body);
        }
      })

    } else if (frame == NSQ.FRAME_TYPE_RESPONSE && data == "__heartbeat_") {
      self.updateHeartbeat()
      conn.send(NSQ.nop())
    }
  }
}

Reader.prototype.handleMessage = function (conn, task, message, cb) {
  conn.ready -= 1;
  var per_conn = this.connectionMaxInFlight();
  try {
    this.sendReady(conn, per_conn);
    this.finish(conn, message.id)
    cb(undefined, conn, task, message);
  } catch (ex) {
    cb(ex);
  }

}

Reader.prototype.sendReady = function (conn, value) {
  conn.send(NSQ.ready(value))
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
  return (1 > this.params.max_in_flight ? 1 : this.params.max_in_flight) / (1 > this.connectionCount ? 1 : this.connectionCount)
}

Reader.prototype.updateHeartbeat = function () {
  console.log('Heart beat.')
}

Reader.prototype.connectToNsqd = function (address, port, task) {
  var conn_id = address + ':' + port + ':' + task;
  if (!this.conns[conn_id]) {
    console.log(address + ':' + port + " connecting to nsqd for " + task);
    this.connectionCount++;
    this.conns[conn_id] = Conn.createClient(address, port, task);
    if (typeof this.params.connect_method == 'function') {
      this.conns[conn_id].on('connect', this.params.connect_method);
    }
    this.conns[conn_id].on('data', this.data);
    this.conns[conn_id].on('close', this.close);
    this.conns[conn_id].on('connect', this.connect);
    this.conns[conn_id].connect();
  }
}

Reader.prototype.queryLookupd = function (cb) {
  var self = this;
  for (var e in this.params.lookupd_http_addresses) {
    var options = {
      host:this.params.lookupd_http_addresses[e].split(':')[0],
      port:this.params.lookupd_http_addresses[e].split(':')[1],
      path:["/lookup?topic=", escape(this.topic)].join(''),
      method:'GET'
    };

    var req = http.request(options, function (res) {
      var data = '';
      if (res.statusCode != 200) {
        console.log('queryLookupd error ' + res.statusCode);
      }
      res.on('data', function (chunk) {
        data += chunk;
      })
      res.on('end', function () {
        if (res.statusCode == 200) {
          data = JSON.parse(data)
          self.finishQueryLookupd(data, cb);
        } else {
          console.log(data);
          if (typeof cb == 'function') {
            cb();
          }
        }
      })
    })
    req.on('error', function (e) {
      console.log('problem with request: ' + e.message);
    });

    req.end();
  }
}

Reader.prototype.finishQueryLookupd = function (lookupData, cb) {
  for (var t in this.task_lookup) {
    var task = this.task_lookup[t];
    for (var p in lookupData['data']['producers']) {
      var producer = lookupData['data']['producers'][p];
      this.connectToNsqd(producer['address'], producer['tcp_port'], t);
    }
  }
  if (typeof cb == 'function') {
    cb()
  }
}

Reader.prototype.run = function () {
  this.poll();
  console.log('Reader running.');
}

exports.Reader = Reader;
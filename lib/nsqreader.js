/**
 * User: philliprosen
 * Date: 10/9/12
 * Time: 4:59 PM
 */

var extend = require('extend'),
  http = require('http'),
  BackOffTimer = require('backofftimer'),
  Conn = require('conn');

function Reader(all_tasks, topic, channel, params) {
  this.all_tasks = all_tasks;
  this.topic = topic;
  this.channel = channel;
  this.backoff_timer = {};
  this.conns = {};

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
    connect_method:false,
  }, params)

  this.requeue_delay = this.params.requeue_delay * 1000;
  this.task_lookup = all_tasks
  this.http_client = http.createClient(this.params.port, this.params.host);

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
//  this.query_lookupd()

  //tornado.ioloop.PeriodicCallback(this.query_lookupd, this.lookupd_poll_interval * 1000).start()
}

Reader.prototype.dataMethod = function () {

}

Reader.prototype.closeMethod = function () {

}

Reader.prototype.handleMessage = function(){

}

Reader.prototype.requeue = function(){

}

Reader.prototype.finish = function(){

}




Reader.prototype.connectToNsqd = function (address, port, task) {

  var conn_id = address + ':' + str(port) + ':' + task;
  if (this.conns.indexOf(conn_id) === -1) {
    console.log(address + ':' + port + " connecting to nsqd for " + task);
    this.conns[conn_id] = Conn.createClient(address, port);
    if (typeof this.params.connect_method == 'function') {
      this.conns[conn_id].on('connect', this.params.connect_method);
    }
    this.conns[conn_id].on('data', this.dataMethod);
    this.conns[conn_id].on('close', this.closeMethod);
    this.conns[conn_id].connect();
  }
}

exports.Reader = Reader;
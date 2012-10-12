var net = require('net')
  , events = require('events')
  , util = require('util');

//exports.Conn = Conn;


function Conn(host, port, task) {
  this.connId =  host + ':' + port+ ':' + task;
  this.host = host || '127.0.0.1';
  this.port = port || 4150;
  this.timeout = false;
  this.connecting = false;
  this.connected = false;
  this.client = false;
  this.task=task;
}

util.inherits(Conn, events.EventEmitter);


Conn.prototype.connect = function () {
  var self = this;
  self.connecting = true;
  var socket = net.connect({host:this.host, port:this.port});
  if (self.timeout) {
    socket.setTimeout(this.timeout);
  }

  socket.on('connect', function () {
    self.connecting = false;
    self.connected = true;
    console.log('connected');
    self.emit('connect',self, self.task);
  })

  socket.on('data', function (data) {
    console.log(data)
    self.emit('data', self, data, self.task);
  })

  socket.on('error', function (error) {
    console.log(error);
    self.emit('error', error);
  })

  socket.on('close', function () {
    self.emit('close',self);
  })
  self.client=socket;
  return self;
}

Conn.prototype.write = function(data,encoding, cb){
  this.client.write(data);
  if(typeof cb=='function'){
    cb();
  }
}

Conn.prototype.send = Conn.prototype.write;


exports.createClient = function(host, port, task){
  return new Conn(host,port,task);
}

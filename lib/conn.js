var net = require('net')
  , events = require('events')
  , util = require('util');

//exports.Conn = Conn;


function Conn(host, port, timeout) {
  this.host = host || '127.0.0.1';
  this.port = port || 4150;
  this.timeout = false || timeout;
  this.connecting = false;
  this.connected = false;
  this.client = false;
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
    self.emit('connect');
  })

  socket.on('data', function (data) {
    self.emit('data', data);
  })

  socket.on('error', function (error) {
    self.emit('error', error);
  })

  socket.on('close', function () {
    self.emit('close');
  })
  self.client=socket;
  return self;
}


exports.createClient = function(host, port, timeout){
  return new Conn(host,port,timeout).connect();
}

/*
 * nsq message stuff
 *
 */
var ref = require('ref')

function _command(cmd) {

  var params = [];

  for (var a = 1; a < arguments.length; a++) {
    params.push(arguments[a]);
  }

  return cmd + " " + params.join(" ") + " \n";
}

function Message(args) {
  this.id = args.id;
  this.body = args.body;
  this.timestamp = args.timestamp;
  this.attempts = args.attempts;
}

exports.unpack_response = function (data) {
  var frame = data.readUInt32BE(0);
  return [frame, data.slice(4, data.length)];
};

exports.decode_message = function (data) {

  //NEED TO FIX - TIMESTAMP NEEDS A 64bit Int

  var timestamp = ref.readUInt64BE(data.slice(0, 6),0),
    attempts = data.slice(6, 8),
    id = data.slice(14, 30);
    body = data.slice(30, data.length);

  return new Message({
    id:id.toString('ascii', 0, id.length),
    body:body.toString('ascii', 0, body.length),
    timestamp:timestamp,
    attempts:attempts.readUInt16BE(0)
  });

};

exports.subscribe = function (topic, channel, short_id, long_id) {
  return _command('SUB', topic, channel, short_id, long_id);
};

exports.ready = function (count) {
  return _command('RDY', count);
};

exports.finish = function (id) {
  return _command('FIN', id);
};

exports.queue = function (id, time_ms) {
  return _command('REQ', id, time_ms);
};

exports.nop = function () {
  return _command('NOP');
};

exports.createMessage = function (args) {
  //error checking for args should be put here
  return new Message(args);
}

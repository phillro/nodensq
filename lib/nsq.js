/*
 * nsq message stuff
 *
 */
var ref = require('ref');
var jspack = new require('node-jspack').jspack;

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
  var frame = -1;
  if(data && data.length>=6){
    frame=data.readUInt32BE(4);
    return [frame, data.slice(4, data.length)];
  }else{
    return [frame, null];
  }

};

exports.decode_message = function (data) {
  var timestamp = ref.readUInt64BE(data.slice(4, 10), 0)
    , attempts = jspack.Unpack('>h', data.slice(12, 14))[0]
    , id = data.slice(14, 30)
    , body = data.slice(30, data.length);

  return new Message({
    id:id.toString('ascii', 0, id.length),
    body:body.toString('ascii', 0, body.length),
    timestamp:timestamp,
    attempts:attempts
  });
};

exports.publish = function(topic, message){
  return _command('PUB',topic, message)
}
exports.subscribe = function (topic, channel, short_id, long_id) {
  return _command('SUB', topic, channel, short_id, long_id);
};

exports.ready = function (count) {
  return _command('RDY', count);
};

exports.finish = function (id) {
  return _command('FIN', id);
};

exports.requeue = function (id, time_ms) {
  return _command('REQ', id, time_ms);
};

exports.nop = function () {
  return _command('NOP');
};

exports.createMessage = function (args) {
  //error checking for args should be put here
  return new Message(args);
}

exports.FRAME_TYPE_RESPONSE = 0
exports.FRAME_TYPE_ERROR = 1
exports.FRAME_TYPE_MESSAGE = 2
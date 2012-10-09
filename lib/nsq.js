/*
 * nsq message stuff
 *
 */

function _command(cmd) {
	var params = arguments;
	params.shift();
	return cmd+" "+params.join("")+" \n";
}


function Message(args) {
	this.id = args.id;
	this.body = args.body;
	this.timestamp = args.timestamp;
	this.attempts = args.attempts;
}

exports.unpack_response = function(date) {
	//implement me
};

exports.decode_message = function(date) {
	//implement me
};

exports.subscribe = function(topic, channel, short_id, long_id) {
  return _command('SUB', topic, channel, short_id, long_id);hhhkkkkk
};


exports.ready = function(count) {
	return _command('RDY', count);
};

exports.finish = function(id) {
	return _command('FIN', id);
};

exports.queue = function(id, time_ms) {
	return _command('REQ', id, time_ms);
};

exports.nop = function() {
	return _command('NOP');
};


exports.createMessage = function(args) {
	//error checking for args should be put here
	return new Message(args);
}

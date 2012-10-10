var nsq  = require(__dirname +"/lib/nsq"),
		conn = require(__dirname + "/lib/conn");


var c = conn.createClient();
c.connect();

c.on("connect",function() {
	c.write("  V2");
	var command = nsq.subscribe("test","nsq_to_file","a","b");
	c.write(command);
	c.write(nsq.ready(10));
});

c.on("data", function(buffer) {
	var a = nsq.unpack_response(buffer);
	console.log(a);
	console.log(nsq.decode_message(a[1]));
});




var through2  = require('through2');
var debug     = require('debug')('stream-to-redis');
var redis     = require('./index');

var s1 = through2.obj();
s1.resume();
s1.pipe(redis.write('tomate'));

var s2 = redis.read('tomate');
s2.pipe(process.stdout)

for (var i=0;i<1000;i++){
  s1.write("message "+i+"\n")
}

process.on('SIGINT', function() {
  s1.end();
  s2.end();
})

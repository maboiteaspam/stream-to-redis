
var pkg = require('./package.json')
process.env['DEBUG'] = pkg.name

var through2  = require('through2');
var debug     = require('debug')(pkg.name);
var redis     = require('./index');

var s1 = through2.obj();
s1.resume();
s1.pipe(redis.write('tomate'));

for (var i=0;i<1000;i++){
  s1.write("message "+i+"\n")
}

var s2 = redis.read('tomate');
s2.pipe(process.stdout)

s2.on('end', function () {
  debug('end of read')
  s1.end();
  //s2.end(); // you may call .end() on a redis-readable to
  // close the connection and end the stream.
  // if you have not passed infinite: true opt to the stream,
  // it will close when all data of the channel was processed
})

//process.nextTick(function () {
//  s2.end(function () {
//    console.log('rrrrr')
//    s1.end();
//    console.log('rrrrr')
//  })
//})

var pkg = require('./package.json')
process.env['DEBUG'] = pkg.name

var through2  = require('through2');
var debug     = require('debug')(pkg.name);
var redis     = require('./index');

var s1 = through2.obj();
s1.resume();
s1.pipe(redis.write('tomate'));

var s2 = redis.read('tomate', {infinity: !true});
s2.pipe(process.stdout)

var wout;
function writewrite(){
  for (var i=0;i<1000;i++){
    s1.write("message "+i+"\n")
  }
  wout = setTimeout(writewrite, 1);
}
writewrite();

s2.on('end', function () {
  debug('end of s2');
  s1.end();
  //s2.end(); // you may call .end() on a redis-readable to
  // close the connection and end the stream.
  // if you have not passed infinite: true opt to the stream,
  // it will close when all data of the channel was processed
})
s1.on('end', function () {
  debug('end of s1');
  clearTimeout(wout);
})

var k = 0
s2.on('data', function (){
  k++;
  if (k===3) {
    //debug('boom')
    //s2.end()
  }
})
//process.nextTick(function () {
//  s2.end(function () {
//    console.log('rrrrr')
//    s1.end();
//    console.log('rrrrr')
//  })
//})

process.on('SIGINT', function () {
  debug('SIGINT')
  s1.end()
  debug('s1.end')
  s2.end(function () {
    debug('s2.end')
  })
})
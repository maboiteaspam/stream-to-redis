# stream to redis

two transforms to write/pull from redis.

It s not data streaming to redis `key`.

It s transforms to read and write into redis as a queue.

# Install

    npm i maboiteaspam/stream-to-redis --save

# Usage

```js
var through2  = require('through2');
var debug     = require('debug')('stream-to-redis');
var redis     = require('steam-to-redis');

var s1 = through2.obj();
s1.resume();
s1.pipe(redis.write('tomate'));

var s2 = redis.read('tomate');
s2.pipe(process.stdout)

for (var i=0;i<1000;i++){
  s1.write("message "+i+"\n")
}

s2.on('end', function () {
  debug('end of read')
  s1.end();
  //s2.end(); // you may call .end() on a redis-readable to
  // close the connection and end the stream.
  // if you have not passed infinity: true opt to the stream,
  // it will close soon.
  // Infinity should provide a mechanism to sub.
})

process.on('SIGINT', function() {
  s1.end();
  s2.end(); // close the underlying redis connection
})
```

# read more

- http://redis.io/commands
- http://www.rediscookbook.org/
- https://github.com/NodeRedis/node_redis
- https://github.com/jeffbski/redis-wstream
- https://github.com/calvinmetcalf/noms
- https://github.com/calvinmetcalf/streams-a-love-story
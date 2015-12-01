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

process.on('SIGINT', function() {
  s1.end();
  s2.end();
})
```

# read more

- http://redis.io/commands
- http://www.rediscookbook.org/
- https://github.com/NodeRedis/node_redis
- https://github.com/jeffbski/redis-wstream
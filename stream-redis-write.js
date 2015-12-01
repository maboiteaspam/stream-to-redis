
var through2 = require('through2');
var debug = require('debug')('stream-to-redis');
var redis = require("redis");


module.exports = function redisWrite(name, opts) {

  opts = opts || {};

  var client = redis.createClient(
    opts.port || 6379,
    opts.host || '0.0.0.0',
    (opts.opts && JSON.parse(opts.opts)) || {

    });

  var allDone = 0;
  var allDoneSoon = function (fn) {
    allDone++;
    return function () {
      var args = [].slice.call(arguments);
      args.push(function(){allDone--;});
      fn ? fn.apply(null, args) : (allDone--);
    }
  };

  var fnTransform = function (chunk, enc, cb) {
    cb(null, chunk);

    var isSerialized = false;
    try{
      chunk = JSON.stringify(chunk);
      isSerialized = true
    } catch(ex) {}

    if (isSerialized) {
      var bucket = name;
      if (!bucket.substr) {
        bucket(client, name, chunk, allDoneSoon(function (bucket, done) {
          client.lpush('bucket-'+bucket, chunk, allDoneSoon(done))
          client.sadd('buckets-'+name, bucket, allDoneSoon())
        }))
      } else {
        client.lpush('bucket-'+bucket, chunk, allDoneSoon())
        client.sadd('buckets-'+name, bucket, allDoneSoon())
      }
    }
  };
  var fnFlush = function (cb) {
    var waitToFlush = function(){
      if (allDone<1) {client.end();cb();}
      else setTimeout(waitToFlush, 10)
    };
    waitToFlush();
  };
  var fnEnd = function () {}
  var fnError = function (err) {
    debug(err)
  }

  var stream = through2.obj(fnTransform, fnFlush)
  //stream.on('end', fnEnd)
  //stream.on('error', fnError)
  stream.resume()

  return stream;
};

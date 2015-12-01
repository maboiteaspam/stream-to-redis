
var through2 = require('through2');
var debug = require('debug')('stream-to-redis');
var redis = require("redis");
var async = require("async");


module.exports = function redisRead (name, opts) {

  opts = opts || {};

  var isEnded = false;
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
    this.push(chunk);
    cb(null);
    process.nextTick(writeOrPull)
  };
  var fnFlush = function (cb) {
    isEnded = true;
    var waitToFlush = function(){
      if (allDone<1) {client.end();cb();}
      else setTimeout(waitToFlush, 5)
    };
    clearTimeout(writeOrPullTout);
    if(currentBucket) client.srem('buckets-acquired-'+name, currentBucket, allDoneSoon());

    waitToFlush();
  };
  var fnEnd = function () {}
  var fnError = function (err) {
    debug(err)
  }

  var stream = through2.obj(fnTransform, fnFlush);

  var client = redis.createClient(
    opts.port || 6379,
    opts.host || '0.0.0.0',
    (opts.opts && JSON.parse(opts.opts)) || {

    });

  var acquire = function (then) {
    client.sdiff('buckets-'+name, 'buckets-acquired-'+name, function (err, found) {
      if (!found.length) return then(null, false);
      var bucket = found.shift();
      client.sadd('buckets-acquired-'+name, bucket, function (err, added) {
        if (!added) return then(null, false);
        then(null, bucket);
      })
    })
  };

  var getBucketData = function (len, then) {
    var asf = [];
    for( var i=0;i<100;i++) {
      asf.push(function (n) {
        client.rpop('bucket-'+currentBucket, function(err, e) {
          if(e) {
            currentData.push(e);
          }
          n();
        })
      })
    }
    //async.series(asf, then)
    var handler = allDoneSoon();
    async.parallelLimit(asf, 10, function () {
      handler();
      then();
    })
  };

  var currentBucket = null;
  var currentData = [];
  var pullMoreData = function (done) {
    if (currentData.length) {
      done()
    } else if (currentBucket) {
      getBucketData(100, function () {
        if (currentData.length) return done();
        client.srem('buckets-acquired-'+name, currentBucket, allDoneSoon());
        client.srem('buckets-'+name, currentBucket, allDoneSoon());
        currentBucket = null;
        done(false)
      })
    } else {
      acquire(function (err, bucket) {
        if (bucket) {
          currentBucket = bucket;
        }
        done(false)
      })
    }
  };

  //stream.on('end', fnEnd)
  //stream.on('error', fnError)
  stream.resume();

  var writeOrPullTout;
  function writeOrPull() {
    if(currentData.length) {
      var d = currentData.shift();
      if(!isEnded) stream.write(JSON.parse(d));
    } else {
      process.nextTick(function () {
        pullMoreData(writeOrPull);
      })
    }
  }

  writeOrPull();

  return stream;
};

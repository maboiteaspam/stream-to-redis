
var debug = require('debug')('stream-to-redis');
var redis = require("redis");
var async = require("async");
var noms = require("noms");

function fromRedis(name, opts) {

  opts = opts || {};

  var queue;

  var before = function (next) {
    queue = new QueueHelper(name, opts)
    queue.open(next)
  };

  var read = function (size, next) {
    debug('size %j', size)

    var that = this;

    queue.getChunk(function (chunks) {
      if (chunks===false) {
        return opts.infinite ? next() : that.push(null);
      }
      do {
        size--;
        that.push(JSON.parse(chunks.shift()));
      }while(size>0 && chunks.length>0);
      next();
    })
  };

  var readable = noms(read, before)

  readable.on('end', function () {
    queue.end()
  })

  readable.end = function (then) {
    queue.end(then)
  }
  return readable
}

function QueueHelper (name, opts) {
  var that = this

  var client = redis.createClient(
    opts.port || 6379,
    opts.host || '0.0.0.0',
    (opts.opts && JSON.parse(opts.opts)) || {

    });

  that.open = function (n) {
    client.on('connect', n)
  }
  that.end = function (then) {
    if (then && client) client.quit(function () {
      client.end()
      then()
    })
    else if(client) client.end()
    else if(then) then()
  }

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
    async.parallelLimit(asf, 10, then)
  };

  var currentBucket = null;
  var currentData = [];

  var release = function (then) {
    async.parallel([
      function(n){client.srem('buckets-acquired-'+name, currentBucket, n)},
      function(n){client.srem('buckets-'+name, currentBucket, n)}
    ], then)
    currentBucket = null;
  }

  this.getChunk = function (done) {
    debug('currentBucket %s', currentBucket)
    debug('currentData.length %s', currentData.length)
    if (currentData.length) {
      done(currentData)
    } else if (currentBucket) {
      getBucketData(100, function () {
        if (currentData.length) return done(currentData);
        release(function () {
          that.getChunk(done)
        })
      })
    } else {
      debug('currentBucket %s', currentBucket)
      acquire(function (err, bucket) {
        if (bucket) {
          debug('newBucket %s', bucket)
          currentBucket = bucket;
          return that.getChunk(done)
        }
        debug('end of all')
        done(false)
      })
    }
  }
}

module.exports = fromRedis;

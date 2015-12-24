
var debug = require('debug')('stream-to-redis');
var redis = require("redis");
var async = require("async");
var noms = require("noms");

function fromRedis(name, opts) {

  opts = opts || {};
  debug('opts %j', opts)

  var queue;
  var qout;
  var hasEnded = true

  var before = function (next) {
    hasEnded = false
    queue = new QueueHelper(name, opts)
    queue.open(next)
  };

  var read = function (size, next) {
    debug('size %j %j', size, hasEnded)

    var that = this;

    if (hasEnded) return that.push(null);

    queue.getChunk(size, function (chunks) {
      if (chunks===false) {
        if (opts.infinity) qout = setTimeout(next, 500)
        else that.push(null);
        return ;
      }
      do {
        size--;
        that.push(JSON.parse(chunks.shift()));
      }while(size>0 && chunks.length>0);
      next();
    })
  };

  var readable = noms(read, before)

  if (opts.secured!==false) {
    process.on('uncaughtException', function(err) {
      queue.end() // this is really important that it occurs anyway.
      // use secured to manage it manually.
    })
  }
  readable.on('error', function () {
    queue.end()
  })

  readable.on('end', function () {
    queue.end()
  })

  readable.end = function () {
    hasEnded = true
  }
  return readable
}


var lock = 0;
var allDoneSoon = function (fn) {
  if (!fn) lock++;
  return function () {
    var args = [].slice.call(arguments);
    args.push(function(){!fn && lock--;});
    fn ? fn.apply(null, args) : (!fn && lock--);
    return true;
  }
};
var waitForAllDoneSoon = function (fn) {
  setTimeout(function (){
    if (lock<=0) fn()
    else waitForAllDoneSoon(fn)
  }, 10)
}

function QueueHelper (name, opts) {
  var that = this

  var hasEnded = true
  var client = redis.createClient(
    opts.port || 6379,
    opts.host || '0.0.0.0',
    (opts.opts && JSON.parse(opts.opts)) || {

    });

  that.open = function (n) {
    hasEnded = false
    client.on('connect', n)
  }
  that.end = function (then) {
    hasEnded = true
    debug('end')
    release(function () {
      debug('release done')
      waitForAllDoneSoon(function(){
        debug('waitForAllDoneSoon')
        client.quit(function () {
          debug('quited')
          client.end()
          if (then) then()
        })
      })
    })
  };

  var acquire = function (then) {
    var unlock = allDoneSoon()
    client.sdiff('buckets-'+name, 'buckets-acquired-'+name, function (err, found) {
      if (!found || !found.length) return unlock() && then(err, false);
      var bucket = found.shift();
      client.sadd('buckets-acquired-'+name, bucket, function (err, added) {
        unlock()
        if (!added) return then(err, false);
        then(err, bucket);
      })
    })
  };

  var getBucketData = function (len, then) {
    var currentData = [];
    var asf = [];
    for( var i=0;i<100;i++) {
      asf.push(function (n) {
        if (hasEnded) return n();
        client.rpop('bucket-'+currentBucket, function(err, e) {
          if(e) {
            currentData.push(e);
          }
          n();
        })
      })
    }
    async.parallelLimit(asf, 500, function (){
      then(currentData)
    })
  };

  var currentBucket = null;

  var release = function (then) {
    if (!currentBucket) return (then && then())
    debug('releasing %s', currentBucket)
    var unlock = allDoneSoon()
    async.parallel([
      function(n){client.srem('buckets-acquired-'+name, currentBucket, n)},
      function(n){client.srem('buckets-'+name, currentBucket, n)}
    ], function (err){
      debug('err %s', err)
      unlock()
      if (then) then(err)
    })
    currentBucket = null;
  }

  this.getChunk = function (size, done) {
    debug('currentBucket %s', currentBucket)
    if (currentBucket) {
      if (hasEnded) {
        debug('hasEnded %s', hasEnded)
        return release(function () {
          done(false)
        })
      }
      getBucketData(size, function (currentData) {
        if (currentData.length) return done(currentData);
        release(function () {
          that.getChunk(size, done)
        })
      })
    } else {
      debug('currentBucket %s', currentBucket)
      acquire(function (err, bucket) {
        if (bucket) {
          debug('newBucket %s', bucket)
          currentBucket = bucket;
          return that.getChunk(size, done)
        }
        debug('acquire failed')
        done(false)
      })
    }
  }
}

module.exports = fromRedis;

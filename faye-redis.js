var Engine = function(server, options) {
    this._server = server;
    this._options = options || {};
      this.logger = options.logger;
      if (!this.logger) {
          var self = this;
          self.logger = {};
          ['info', 'error'].forEach(function(key) {
              self.logger[key] = function() {};
          })
      }
      
    var redis = require('ioredis'),
        db = this._options.database || this.DEFAULT_DATABASE,
        auth = this._options.password,
        gc = this._options.gc || this.DEFAULT_GC,
        socket = this._options.socket;

    this._ns = this._options.namespace || '';

    if (socket) {
        this._redis = redis.createClient(socket, { no_ready_check: true });
        this._subscriber = redis.createClient(socket, { no_ready_check: true });
    } else {
        this._redis = createRedis.call(this, this._options);
        this._subscriber = createRedis.call(this, this._options);
    }

  //   if (auth) {
  //       this._redis.auth(auth);
  //       this._subscriber.auth(auth);
  //   }
  //   this._redis.select(db);
  //   this._subscriber.select(db);

    this._messageChannel = this._ns + '/notifications/messages';
    this._closeChannel = this._ns + '/notifications/close';

    var self = this;
    this._subscriber.subscribe(this._messageChannel);
    this._subscriber.subscribe(this._closeChannel);
    this._subscriber.on('message', function(topic, message) {
        if (topic === self._messageChannel) self.emptyQueue(message);
        if (topic === self._closeChannel) self._server.trigger('close', message);
    });

    function isSentinels(config) {
          return config.hasOwnProperty('sentinels');
    }

    function createRedis(config) {
      // return new redis.Cluster([{
      //     "host": "192.168.100.94",
      //     "port": "6375",
      //   }, {
      //     "host": "192.168.100.94",
      //     "port": "7001",
      //   }, {
      //     "host": "192.168.100.95",
      //     "port": "6376",
      //   }, {
      //     "host": "192.168.100.95",
      //     "port": "7002",
      //   }, {
      //     "host": "192.168.100.96",
      //     "port": "6377",
      //   }, {
      //     "host": "192.168.100.96",
      //     "port": "7003",
      //   }], {
      //     redisOptions: {
      //       db: 0
      //     }
      //   })
        var option = {db: config.database};


          if (isSentinels(config)) {
              /** 哨兵模式 */
              var sentinelsConfig = config.sentinels;
              option.sentinels = sentinelsConfig.remote;
              option.name = sentinelsConfig.name;
          } else {
              var port = config.port || this.DEFAULT_PORT,
                  host = config.host || this.DEFAULT_HOST;
              option.port = port;
              option.host = host;
              // return new redis(
              //     port,
              //     host,
              //     { no_ready_check: true, socket_keepalive: true }
              // )
          }
          if (config.password) {
              option.password = config.password;
          }
          return new redis(option)
    }

    function generateUUID() {
        var d = new Date().getTime();
        if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
            d += performance.now();
        }
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = (d + Math.random() * 16) % 16 | 0;
            d = Math.floor(d / 16);
            return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
        });
    }

    function createGCTask(uniFileId) {
        var expiryTime = gc * 2;
        var gcSchedule = function() {
          
            self._gc = setInterval(function() {
                self._redis.setex(self._ns + '/locks/singleton', expiryTime, uniFileId);
                // self._redis.expire(self._ns + '/locks/singleton', expiryTime);
                self.gc();
            }, gc * 1000);
        }
        self._redis.set(self._ns + '/locks/singleton', uniFileId, 'NX', 'EX', expiryTime, function(error, set) {
            if (set) {
                //给外部应用设置一个集群模式下单列调用
                if(self._options.singleProcess){
                  self._options.singleProcess(uniFileId,uniFileId);
                }
                gcSchedule();
            } else {
                self._redis.get(self._ns + '/locks/singleton', function(error, oldUniFileId) {
                     //给外部应用设置一个集群模式下单列调用
                    if(self._options.singleProcess){
                      self._options.singleProcess(uniFileId,oldUniFileId);
                    }
                    if (oldUniFileId == uniFileId) {
                        gcSchedule();
                    }
                })
            }

        })
    }



    //support cluster mode
    if (require("cluster").isMaster) {
        var fs = require("fs"),
            uniFile = "/tmp/faye.uni";

        fs.readFile(uniFile, function(err, data) {
            if (err) {
                var uniFileId = generateUUID();
                fs.writeFile(uniFile, uniFileId, function() {
                    createGCTask(uniFileId);
                })
            } else {
                createGCTask(data.toString());
            }
        });



    }
};

Engine.create = function(server, options) {
    return new this(server, options);
};

Engine.prototype = {
    DEFAULT_HOST: 'localhost',
    DEFAULT_PORT: 6379,
    DEFAULT_DATABASE: 0,
    DEFAULT_GC: 60,
    LOCK_TIMEOUT: 120,

    disconnect: function() {
        this._redis.end();
        this._subscriber.unsubscribe();
        this._subscriber.end();
        if (this._gc) {
            clearInterval(this._gc);
        }
    },

    createClient: function(callback, context, message) {
        var clientId = this._server.generateId(),
            self = this;
        if (message && typeof(message.clientType) != "undefined") {
            clientId = message.clientType + clientId;
        }
        this._redis.zadd(this._ns + '/clients', 0, clientId, function(error, added) {
            if (added === 0) return self.createClient(callback, context);
            self._server.debug('Created new client ?', clientId);
            self.ping(clientId);
            self._server.trigger('handshake', clientId);
            callback.call(context, clientId);
        });
    },

    clientExists: function(clientId, callback, context) {
        var cutoff = new Date().getTime() - (1000 * 1.6 * this._server.timeout);

        this._redis.zscore(this._ns + '/clients', clientId, function(error, score) {
            callback.call(context, parseInt(score, 10) > cutoff);
        });
    },

    destroyClient: function(clientId, callback, context) {
        var self = this;

        this._redis.smembers(this._ns + '/clients/' + clientId + '/channels', function(error, channels) {
          var r = self._redis;
          var sremPromise = Promise.all(channels.map(function(channel) {
              return r.srem(self._ns + '/channels' + channel, clientId)
                  .then(function() {
                      return r.srem(self._ns + '/clients/' + clientId + '/channels', channel);
                  })
          }))

          Promise.all([
              r.zadd(self._ns + '/clients', 0, clientId),
              sremPromise,
              r.del(self._ns + '/clients/' + clientId + '/messages'),
              r.zrem(self._ns + '/clients', clientId),
              r.publish(self._closeChannel, clientId)
          ]).then(function(result) {
              var successRem = result[1];
              channels.forEach(function(channel, i) {
                  if (successRem[i]) {
                      self._server.trigger('unsubscribe', clientId, channel);
                      self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
                  }
              })
              
              self._server.trigger('disconnect', clientId);
              self._server.debug('Destroyed client ?', clientId);

              if (callback) callback.call(context);
          })
        });
    },

    ping: function(clientId) {
        var timeout = this._server.timeout;
        if (typeof timeout !== 'number') return;

        var time = new Date().getTime();

        this._server.debug('Ping ?, ?', clientId, time);
        this._redis.zadd(this._ns + '/clients', time, clientId);
    },

    subscribe: function(clientId, channel, callback, context) {
        var self = this;
        this._redis.sadd(this._ns + '/clients/' + clientId + '/channels', channel, function(error, added) {
            if (added === 1) self._server.trigger('subscribe', clientId, channel);
        });
        this._redis.sadd(this._ns + '/channels' + channel, clientId, function() {
            self._server.debug('Subscribed client ? to channel ?', clientId, channel);
            if (callback) callback.call(context);
        });
    },

    unsubscribe: function(clientId, channel, callback, context) {
        var self = this;
        this._redis.srem(this._ns + '/clients/' + clientId + '/channels', channel, function(error, removed) {
            if (removed === 1) self._server.trigger('unsubscribe', clientId, channel);
        });
        this._redis.srem(this._ns + '/channels' + channel, clientId, function() {
            self._server.debug('Unsubscribed client ? from channel ?', clientId, channel);
            if (callback) callback.call(context);
        });
    },

    publish: function(message, channels) {

        this._server.debug('Publishing message ?', message);

        var self = this,
            jsonMessage = JSON.stringify(message),
            keys = channels.map(function(c) {
                return self._ns + '/channels' + c
            });

        Promise.all(keys.map(function(key) {
          return self._redis.smembers(key)
        })).then(function(clients) {
          clients = clients.reduce(function(acc, item) {
              return acc.concat(item);
          }, [])
          clients.forEach(function(clientId) {
              var queue = self._ns + '/clients/' + clientId + '/messages';

              self._server.debug('Queueing for client ?: ?', clientId, message);
              self._redis.rpush(queue, jsonMessage);
              self._redis.publish(self._messageChannel, clientId);

              self.clientExists(clientId, function(exists) {
                  if (!exists) self._redis.del(queue);
              });
          });
        }).catch(function(err) {
            console.log('test  error =>', err);
        })
        this._server.trigger('publish', message.clientId, message.channel, message.data);
    },

    emptyQueue: function(clientId) {
          if (!this._server.hasConnection(clientId)) return;
          var key = this._ns + '/clients/' + clientId + '/messages',
          self = this;
          this._redis.lrange(key, 0, -1).then(function(result) {
              var messages = result.map(function(json) {
                  return JSON.parse(json);
              })
              console.log('empty queue ==>', messages);
              console.log('================================================');
              return self._server.deliver(clientId, messages);
          })
          .then(function() {
              self._redis.del(key);
          })
    },

    gc: function() {
        var timeout = this._server.timeout;
        if (typeof timeout !== 'number') return;

        this._withLock('gc', function(releaseLock) {
            var cutoff = new Date().getTime() - 1000 * 2 * timeout,
                self = this;

            this._redis.zrangebyscore(this._ns + '/clients', 0, cutoff, function(error, clients) {
                var i = 0,
                    n = clients.length;
                if (i === n) return releaseLock();

                clients.forEach(function(clientId) {
                    this.destroyClient(clientId, function() {
                        i += 1;
                        if (i === n) releaseLock();
                    }, this);
                }, self);
            });
        }, this);
    },

    _withLock: function(lockName, callback, context) {
        var lockKey = this._ns + '/locks/' + lockName,
            currentTime = new Date().getTime(),
            expiry = currentTime + this.LOCK_TIMEOUT * 1000 + 1,
            self = this;

        var releaseLock = function() {
            if (new Date().getTime() < expiry) self._redis.del(lockKey);
        };

        this._redis.setnx(lockKey, expiry, function(error, set) {
            if (set === 1) return callback.call(context, releaseLock);

            self._redis.get(lockKey, function(error, timeout) {
                if (!timeout) return;

                var lockTimeout = parseInt(timeout, 10);
                if (currentTime < lockTimeout) return;

                self._redis.getset(lockKey, expiry, function(error, oldValue) {
                    if (oldValue !== timeout) return;
                    callback.call(context, releaseLock);
                });
            });
        });
    }
};

module.exports = Engine;
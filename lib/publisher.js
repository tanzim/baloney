'use strict';

var assert = require('assert');
var Redis = require('ioredis');
var _ = require('lodash');

var Job = require('./job');

//
// An empty lists does not exist in redis, therefore we need another mechanism
// to tell the queue that it is paused, the 'meta-paused' key.
//
function addJob(queue, jobId, opts) {
  var push = (opts.lifo ? 'R' : 'L') + 'PUSH';
  var script = [
    'if redis.call("EXISTS", KEYS[3]) ~= 1 then',
    ' redis.call("' + push + '", KEYS[1], ARGV[1])',
    'else',
    ' redis.call("' + push + '", KEYS[2], ARGV[1])',
    'end',
    'redis.call("PUBLISH", KEYS[4], ARGV[1])'
    ].join('\n');

  var keys = _.map(['wait', 'paused', 'meta-paused', 'jobs'], function(name) {
    return queue.toKey(name);
  });

  return queue.client.eval(script, keys.length, keys[0], keys[1], keys[2], keys[3], jobId);
}

var Publisher = function Publisher(name, clientOrUrl) {
  if (!(this instanceof Publisher)) {
    return new Publisher(name, clientOrUrl);
  }

  var client;
  this.name = name;
  this.ownClient = false;

  if (typeof clientOrUrl === 'string') {
    client = new Redis(clientOrUrl);
    this.ownClient = true;
  } else {
    client = clientOrUrl;
  }

  this.client = client;
};

/**
  Adds a job to the Publisher.
  @method add
  @param data: {} Custom data to store for this job. Should be JSON serializable.
  @param opts: JobOptions Options for this job.
*/
Publisher.prototype.add = function(data, opts, callback) {
  assert(typeof data === 'object');
  opts = opts || {};
  callback = callback || function noop() {};

  var _this = this;

  this.client.incr(this.toKey('id'), function onIncr(error, jobId) {
    if (error) return callback(error);
    var job = new Job(jobId, data, opts);
    _this.client.hmset(_this.toKey(jobId), job.toData(), function(error) {
      if (error) return callback(error);
      return addJob(_this, job.jobId, opts, callback);
    });
  });
};

/**
 *  Forcibly closes the Redis client connection
 */
Publisher.prototype.disconnect = function() {
  if (this.ownClient) this.client.disconnect();
};

/**
 *  Closes (quits) the Redis client connection
 */
Publisher.prototype.close = function() {
  if (this.ownClient) this.client.quit();
};

Publisher.prototype.toKey = function(queueType) {
  return 'bull:' + this.name + ':' + queueType;
};

Publisher.create = function(name, clientOrUrl) {
  return new Publisher(name, clientOrUrl);
};

module.exports = Publisher;

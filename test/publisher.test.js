'use strict';

var Queue = require('bull');
var expect = require('expect.js');
var Redis = require('ioredis');

var Publisher = require('../lib/publisher');

var DEFAULT_QUEUE_NAME = 'default';

function createPublisher(redis) {
  return Publisher.create(DEFAULT_QUEUE_NAME, redis);
}

function buildQueue(name, redis) {
  return new Queue(DEFAULT_QUEUE_NAME, redis);
}

function cleanupQueue(queue) {
  return queue.empty().then(queue.close.bind(queue));
}

describe('Queue', function() {
  var queue;
  var redis;

  describe('instantiation', function() {
    // it('should create a queue with standard redis opts', function(done) {
    //   queue = new Queue('standard');

    //   queue.once('ready', function() {
    //     expect(queue.client.connectionOption.host).to.be('127.0.0.1');
    //     expect(queue.client.connectionOption.port).to.be(6379);

    //     expect(queue.client.selected_db).to.be(0);
    //     expect(queue.bclient.selected_db).to.be(0);

    //     done();
    //   });
    // });

    // it('creates a queue using the supplied redis DB', function(done) {
    //   queue = new Queue('custom', { redis: { DB: 1 } });

    //   queue.once('ready', function() {
    //     expect(queue.client.connectionOption.host).to.be('127.0.0.1');

    //     expect(queue.client.connectionOption.port).to.be(6379);

    //     expect(queue.client.selected_db).to.be(1);

    //     done();
    //   });
    // });

    // it('creates a queue using custom the supplied redis host', function(done) {
    //   queue = new Queue('custom', { redis: { host: 'localhost' } });

    //   queue.once('ready', function() {
    //     expect(queue.client.connectionOption.host).to.be('localhost');
    //     expect(queue.client.selected_db).to.be(0);
    //     done();
    //   });
    // });
  });

  describe(' a worker', function() {

    beforeEach(function(done) {
      redis = new Redis();
      done();
    });

    afterEach(function(done) {
      redis.close();
      done();
    });

    it('should process a job', function(done) {
      queue = buildQueue();
      queue.process(function(job, jobDone) {
        expect(job.data.foo).to.be.equal('bar');
        jobDone();
        done();
      });

      var publisher = createPublisher(redis);
      publisher.add({ foo: 'bar' }, function(error, job) {
        if (error) return done(error);

        expect(job.jobId).to.be.ok();
        expect(job.data.foo).to.be('bar');

        done(error);
      });
    });

    it('process a job that returns data in the process handler', function(done) {
      queue = buildQueue();
      queue.process(function(job, jobDone) {
        expect(job.data.foo).to.be.equal('bar');
        jobDone(null, 37);
      });

      var publisher = createPublisher(redis);
      publisher.add({ foo: 'bar' }, function(job) {
        expect(job.jobId).to.be.ok();
        expect(job.data.foo).to.be('bar');
      });

      queue.on('completed', function(job, data) {
        expect(job).to.be.ok();
        expect(data).to.be.eql(37);
        done();
      });
    });

  //   it('process a synchronous job', function(done) {
  //     queue = buildQueue();
  //     queue.process(function(job) {
  //       expect(job.data.foo).to.be.equal('bar');
  //     });

  //     queue.add({ foo: 'bar' }).then(function(job) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //     }).catch(done);

  //     queue.on('completed', function(job) {
  //       expect(job).to.be.ok();
  //       done();
  //     });
  //   });

  //   it('process stalled jobs when starting a queue', function(done) {
  //     var queueStalled = new Queue('test queue stalled', 6379, '127.0.0.1');
  //     queueStalled.LOCK_RENEW_TIME = 10;
  //     var jobs = [
  //       queueStalled.add({ bar: 'baz' }),
  //       queueStalled.add({ bar1: 'baz1' }),
  //       queueStalled.add({ bar2: 'baz2' }),
  //       queueStalled.add({ bar3: 'baz3' })
  //     ];

  //     Promise.all(jobs).then(function() {
  //       queueStalled.process(function() {
  //         // instead of completing we just close the queue to simulate a crash.
  //         queueStalled.disconnect();
  //         setTimeout(function() {
  //           var queue2 = new Queue('test queue stalled', 6379, '127.0.0.1');
  //           var doneAfterFour = _.after(4, function() {
  //             done();
  //           });
  //           queue2.on('completed', doneAfterFour);

  //           queue2.process(function(job, jobDone) {
  //             jobDone();
  //           });
  //         }, 100);
  //       });
  //     });
  //   });

  //   it('processes several stalled jobs when starting several queues', function(done) {
  //     var NUM_QUEUES = 10;
  //     var NUM_JOBS_PER_QUEUE = 20;
  //     var stalledQueues = [];
  //     var jobs = [];

  //     for (var i = 0; i < NUM_QUEUES; i++) {
  //       var queueStalled2 = new Queue('test queue stalled 2', 6379, '127.0.0.1');
  //       stalledQueues.push(queueStalled2);
  //       queueStalled2.LOCK_RENEW_TIME = 10;

  //       for (var j = 0; j < NUM_JOBS_PER_QUEUE; j++) {
  //         jobs.push(queueStalled2.add({ job: j }));
  //       }
  //     }

  //     Promise.all(jobs).then(function() {
  //       var processed = 0;
  //       var procFn = function() {
  //         // instead of completing we just close the queue to simulate a crash.
  //         this.disconnect();

  //         processed++;
  //         if (processed === stalledQueues.length) {
  //           setTimeout(function() {
  //             var queue2 = new Queue('test queue stalled 2', 6379, '127.0.0.1');
  //             queue2.process(function(job2, jobDone) {
  //               jobDone();
  //             });

  //             var counter = 0;
  //             queue2.on('completed', function() {
  //               counter++;
  //               if (counter === NUM_QUEUES * NUM_JOBS_PER_QUEUE) {
  //                 queue2.close().then(function() {
  //                   done();
  //                 });
  //               }
  //             });
  //           }, 100);
  //         }
  //       };

  //       for (var k = 0; k < stalledQueues.length; k++) {
  //         stalledQueues[k].process(procFn);
  //       }
  //     });
  //   });

  //   it('does not process a job that is being processed when a new queue starts', function(done) {
  //     this.timeout(5000);
  //     var err = null;
  //     var anotherQueue;

  //     queue = buildQueue();

  //     queue.add({ foo: 'bar' }).then(function(addedJob) {
  //       queue.process(function(job, jobDone) {
  //         expect(job.data.foo).to.be.equal('bar');

  //         if (addedJob.jobId !== job.jobId) {
  //           err = new Error('Processed job id does not match that of added job');
  //         }

  //         setTimeout(jobDone, 100);
  //       });
  //       setTimeout(function() {
  //         anotherQueue = buildQueue();
  //         anotherQueue.process(function(job, jobDone) {
  //           err = new Error('The second queue should not have received a job to process');
  //           jobDone();
  //         });

  //         queue.on('completed', function() {
  //           cleanupQueue(anotherQueue).then(done.bind(null, err));
  //         });
  //       }, 10);
  //     });
  //   });

  //   it('process stalled jobs without requiring a queue restart', function(done) {
  //     this.timeout(5000);
  //     var collect = _.after(2, done);

  //     queue = buildQueue('running-stalled-job-' + uuid());

  //     queue.LOCK_RENEW_TIME = 1000;

  //     queue.on('completed', function() {
  //       collect();
  //     });

  //     queue.process(function(job, jobDone) {
  //       expect(job.data.foo).to.be.equal('bar');
  //       jobDone();
  //       var client = redis.createClient();
  //       client.srem(queue.toKey('completed'), 1);
  //       client.lpush(queue.toKey('active'), 1);
  //     });

  //     queue.add({ foo: 'bar' }).then(function(job) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //     }).catch(done);
  //   });

  //   it('process a job that fails', function(done) {
  //     var jobError = new Error('Job Failed');
  //     queue = buildQueue();

  //     queue.process(function(job, jobDone) {
  //       expect(job.data.foo).to.be.equal('bar');
  //       jobDone(jobError);
  //     });

  //     queue.add({ foo: 'bar' }).then(function(job) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //     }, function(err) {
  //         done(err);
  //       });

  //     queue.once('failed', function(job, err) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //       expect(err).to.be.eql(jobError);
  //       done();
  //     });
  //   });

  //   it('process a job that throws an exception', function(done) {
  //     var jobError = new Error('Job Failed');

  //     queue = buildQueue();

  //     queue.process(function(job) {
  //       expect(job.data.foo).to.be.equal('bar');
  //       throw jobError;
  //     });

  //     queue.add({ foo: 'bar' }).then(function(job) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //     }, function(err) {
  //         done(err);
  //       });

  //     queue.once('failed', function(job, err) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //       expect(err).to.be.eql(jobError);
  //       done();
  //     });
  //   });

  //   it('process a job that returns a rejected promise', function(done) {
  //     var jobError = new Error('Job Failed');
  //     queue = buildQueue();

  //     queue.process(function(job) {
  //       expect(job.data.foo).to.be.equal('bar');
  //       return Promise.reject(jobError);
  //     });

  //     queue.add({ foo: 'bar' }).then(function(job) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //     }, function(err) {
  //       done(err);
  //     });

  //     queue.once('failed', function(job, err) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //       expect(err).to.be.eql(jobError);
  //       done();
  //     });
  //   });

  //   it('retry a job that fails', function(done) {
  //     var called = 0;
  //     var messages = 0;
  //     var failedOnce = false;

  //     var retryQueue = buildQueue('retry-test-queue');
  //     var client = redis.createClient(6379, '127.0.0.1', {});

  //     client.select(0);

  //     client.on('ready', function() {
  //       client.on('message', function(channel, message) {
  //         expect(channel).to.be.equal(retryQueue.toKey('jobs'));
  //         expect(parseInt(message, 10)).to.be.a('number');
  //         messages++;
  //       });
  //       client.subscribe(retryQueue.toKey('jobs'));

  //       retryQueue.add({ foo: 'bar' }).then(function(job) {
  //         expect(job.jobId).to.be.ok();
  //         expect(job.data.foo).to.be('bar');
  //       });
  //     });

  //     retryQueue.process(function(job, jobDone) {
  //       called++;
  //       if (called % 2 !== 0) {
  //         throw new Error('Not even!');
  //       }

  //       jobDone();
  //     });

  //     retryQueue.once('failed', function(job, err) {
  //       expect(job.jobId).to.be.ok();
  //       expect(job.data.foo).to.be('bar');
  //       expect(err.message).to.be.eql('Not even!');
  //       failedOnce = true;
  //       retryQueue.retryJob(job);
  //     });

  //     retryQueue.once('completed', function() {
  //       expect(failedOnce).to.be(true);
  //       expect(messages).to.eql(2);
  //       done();
  //     });
  //   });

  //   it('process several jobs serially', function(done) {
  //     var counter = 1;
  //     var maxJobs = 100;

  //     queue = buildQueue();

  //     queue.process(function(job, jobDone) {
  //       expect(job.data.num).to.be.equal(counter);
  //       expect(job.data.foo).to.be.equal('bar');
  //       jobDone();
  //       if (counter === maxJobs) {
  //         done();
  //       }

  //       counter++;
  //     });

  //     for (var i = 1; i <= maxJobs; i++) {
  //       queue.add({ foo: 'bar', num: i });
  //     }
  //   });

  //   it('process a lifo queue', function(done) {
  //     var currentValue = 0, first = true;
  //     queue = new Queue('test lifo');

  //     queue.once('ready', function() {
  //       queue.process(function(job, jobDone) {
  //         // Catching the job before the pause
  //         if (first) {
  //           expect(job.data.count).to.be.equal(0);
  //           first = false;
  //           return jobDone();
  //         }

  //         expect(job.data.count).to.be.equal(currentValue--);
  //         jobDone();
  //         if (currentValue === 0) {
  //           done();
  //         }
  //       });

  //       // Add a job to pend proccessing
  //       queue.add({ 'count': 0 }).then(function() {
  //         queue.pause().then(function() {
  //           // Add a series of jobs in a predictable order
  //           var fn = function(cb) {
  //             queue.add({ 'count': ++currentValue }, { 'lifo': true }).then(cb);
  //           };
  //           fn(fn(fn(fn(function() {
  //             queue.resume();
  //           }))));
  //         });
  //       });
  //     });
  //   });
  });

  // it('count added, unprocessed jobs', function() {
  //   var maxJobs = 100;
  //   var added = [];

  //   queue = buildQueue();

  //   for (var i = 1; i <= maxJobs; i++) {
  //     added.push(queue.add({ foo: 'bar', num: i }));
  //   }

  //   return Promise.all(added)
  //     .then(queue.count.bind(queue))
  //     .then(function(count) {
  //     expect(count).to.be(100);
  //   })
  //     .then(queue.empty.bind(queue))
  //     .then(queue.count.bind(queue))
  //     .then(function(count) {
  //     expect(count).to.be(0);
  //   });
  // });

  // it('emits waiting event when a job is added', function(cb) {
  //   queue = buildQueue();
  //   queue.add({ foo: 'bar' });
  //   queue.once('waiting', function(job) {
  //     expect(job.data.foo).to.be.equal('bar');
  //     cb();
  //   });
  // });

});

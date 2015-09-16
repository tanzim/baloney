'use strict';

var Job = function(jobId, data, opts) {
  opts = opts || {};
  this.jobId = jobId;
  this.data = data;
  this.opts = opts;
  this._progress = 0;
  this.delay = this.opts.delay;
  this.timestamp = opts.timestamp || Date.now();
  this.stacktrace = null;
};

Job.prototype.toData = function() {
  return {
    data: JSON.stringify(this.data || {}),
    opts: JSON.stringify(this.opts || {}),
    progress: this._progress,
    delay: this.delay,
    timestamp: this.timestamp
  };
};

module.exports = Job;

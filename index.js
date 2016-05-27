'use strict';

let db = require('./lib/db'),
    co = require('co'),
    config = require('./lib/config'),
    scheduler = require('./lib/scheduler'),
    Runner = require('./lib/runner');

module.exports = {
    /**
     * Started runners.
     * @private
     */
    _runners: [],

    /**
     * Graceful shutdown is in progress
     * @private
     */
    _isStopping: false,

    /**
     * Set url for connection to mongo. Real connection will be created as soon as it will try to execute any query
     * @param {string} url - url to mongo db
     * @param {Object} [options]
     * @param {string} [options.collection] - specify collection name that should be used by default
     * @param {Object} [options.logger] - specify logger that should be used
     */
    connect: function (url, options) {
        options = options || {};

        if (options.collection) {
            config.collection = options.collection;
        }

        if (options.logger) {
            config.logger = options.logger;
        }

        return co(function* () {
            yield db.connect(url);
        });
    },

    /**
     * Force close connection to mongo
     * Usually you don't need it
     * @returns {Promise}
     */
    close: function () {
        return co(function* () {
            yield db.close();
        });
    },

    /**
     * @param {object} query
     * @param {string} [collection] - by default 'tasks'
     * @returns {Promise}
     */
    remove: function (query, collection) {
        collection = collection || config.collection;

        return co(function* () {
            yield db.remove(collection, query);
        });
    },

    /**
     * @param {object} query
     * @param {string} [collection] - by default 'tasks'
     * @returns {Promise}
     */
    findTask: function (query, collection) {
        collection = collection || config.collection;

        return co(function* () {
            return yield db.findTask(collection, query);
        });
    },

    /**
     * @param {string} name - task name
     * @param {*} data - task data, will be passed into task as an argument
     * @param {Object} [options]
     * @param {string} [options.taskId] - by default uuid will be generated
     * @param {string} [options.group] - null by default
     * @param {Date}   [options.startAt] - in UTC (current date by default - will be executed immediately)
     * @param {number} [options.repeatEvery] - in seconds, (0 by default - disabled)
     * @param {string} [options.retryStrategy] - one of 'none', 'pow1', 'pow2', 'pow3'
     * @param {string} [options.collection] - "tasks" by default
     * @returns {Promise}
     */
    schedule: function (name, data, options) {
        return co(function* () {
            return yield scheduler.scheduleTask(name, data, options);
        });
    },

    /**
     * @param {object} [options]
     * @param {number} [options.scanInterval] - in seconds, 60 by default
     * @param {number} [options.lockInterval] - in seconds, 60 by default
     * @param {function} [options.taskProcessorFactory] - should return task processor by task name, require() by default
     * @param {number} [options.tasksPerScanning] - count of tasks that should be picked per every scanning. By default 1000
     * @param {string} [options.collection] - "tasks" by default
     * @returns {Promise}
     */
    run: function (options) {
        let runner = new Runner(options);
        this._runners.push(runner);

        return co(function* () {
            yield runner.start();
        });
    },

    /**
     * Graceful shutdown.
     * Runners will have 30 seconds to finish current tasks.
     * If tasks will be not finished in 30 seconds - force process shutdown.
     * @returns {Promise}
     */
    stop: function() {
        let self = this;

        return co(function () {
            config.logger.debug('Graceful shutdown is in progress, please wait...');

            if (self._isStopping) {
                return;
            }

            function _gracefulShutdown(secondsPassed) {
                if ( secondsPassed === 30 ) {
                    process.exit(1);
                }

                this._runners = this._runners.filter(runner => !runner.isStopped);

                if ( this._runners.length ) {
                    secondsPassed++;
                    setTimeout(_gracefulShutdown.bind(this, secondsPassed), 1000);

                } else {
                    return this.close();
                }
            }

            self._isStopping = true;
            self._runners.forEach(runner => runner.stop());

            return _gracefulShutdown.call(self, 0);
        });
    }
};

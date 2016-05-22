'use strict';

let db = require('./lib/db'),
    co = require('co'),
    uuid = require('uuid'),
    scheduler = require('./lib/scheduler'),
    runner = require('./lib/runner');

module.exports = {
    /**
     * Set url for connection to mongo. Real connection will be created as soon as it will try to execute any query
     * @param {string} url - url to mongo db
     */
    connect: function (url) {
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
        collection = collection || 'tasks';

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
        collection = collection || 'tasks';

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
        let self = this,
            _options = Object.assign({
                scanInterval: 60, // seconds
                lockInterval: 60, // seconds
                taskProcessorFactory: require,
                tasksPerScanning: 1000, // tasks that will be executed per every scanning iteration.
                collection: 'tasks'
            }, options);

        return co(function* () {
            yield runner.run(_options);

        }).catch(function(err) {
            // something goes wrong, probably missed connection to mongo - log error
            console.log('tasks-runner: error', err.stack);

        }).then(function() {
            // schedule new scanning
            console.log('tasks-runner: finished scanning iteration, rescan in ' + options.scanInterval + ' seconds.');
            setTimeout(co.wrap(self.run.bind(self, _options)), _options.scanInterval * 1000);
        });
    }
};

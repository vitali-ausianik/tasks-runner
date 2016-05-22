'use strict';

let db = require('./db'),
    uuid = require('uuid'),
    retryStrategies = ['none', 'powN', 'Nm', 'Nh', 'Nd'];

module.exports = {
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
     */
    scheduleTask: function* (name, data, options) {
        let _options;

        if (!name || 'string' !== typeof name) {
            throw new Error('Name should be of type "string", found "' + typeof name + '"');
        }

        if (options && options.taskId && 'string' !== typeof options.taskId) {
            throw new Error('Option "taskId" should be of type "string", found "' + typeof options.taskId + '"');
        }

        if (options && options.retryStrategy && 'string' !== typeof options.retryStrategy) {
            throw new Error(
                'Option "retryStrategy" should be of type string, found "' + typeof options.retryStrategy + '"'
            );
        }

        _options = Object.assign({
            taskId: uuid.v4(),
            group: null,
            startAt: new Date(),
            repeatEvery: 0,
            retryStrategy: 'pow1',
            collection: 'tasks'
        }, options);

        let isRetryStrategyExists = retryStrategies.some((pattern) => {
            let regexp = new RegExp('^' + pattern.replace('N', '\\d') + '$', 'gi');
            return regexp.test(_options.retryStrategy);
        });

        if (!isRetryStrategyExists) {
            throw new Error(
                'Option "retryStrategy" should be matched with one of following patterns [' +
                retryStrategies.join(',') + '], where N means any integer. Found "' + options.retryStrategy + '"'
            );
        }

        // Don't allow to set group for repeatable task
        if (_options.repeatEvery > 0 && _options.group) {
            throw new Error('Can not specify group for repeatable task');
        }

        return yield db.addTask(_options.collection, {
            taskId: _options.taskId,
            name: name,
            data: data,
            group: _options.group,
            startAt: _options.startAt,
            repeatEvery: _options.repeatEvery,
            retryStrategy: _options.retryStrategy
        });
    },

    /**
     * Reschedule failed task based on its retryStrategy
     *
     * @param {string} collection
     * @param {Object} task
     */
    rescheduleFailedTask: function* (collection, task) {
        let delta;
        if (-1 !== task.retryStrategy.search(/^pow\d$/gi)) {
            // reschedule task in (retries+1)^N minutes
            let pow = parseInt(task.retryStrategy.replace('pow', ''));
            delta = Math.pow(task.retries + 1, pow);

        } else if (-1 !== task.retryStrategy.search(/^\dm$/gi)) {
            // reschedule task in N minutes
            delta = parseInt(task.retryStrategy);

        } else if (-1 !== task.retryStrategy.search(/^\dh$/gi)) {
            // reschedule task in N hours
            delta = parseInt(task.retryStrategy) * 60;

        } else if (-1 !== task.retryStrategy.search(/^\dd$/gi)) {
            // reschedule task in N days
            delta = parseInt(task.retryStrategy) * 24 * 60;
        }

        if (delta) {
            delta = delta * 60 * 1000; // convert delta from minutes to milliseconds
            let scheduleAt = new Date(Date.now() + delta);
            yield db.rescheduleTask(collection, task.taskId, scheduleAt);
        }
    },

    /**
     * Reschedule repeatable task based on its repeatEvery
     * @param {string} collection
     * @param {Object} task
     */
    rescheduleRepeatableTask: function* (collection, task) {
        let scheduleAt = new Date(Date.now() + (task.repeatEvery * 1000));
        yield db.rescheduleTask(collection, task.taskId, scheduleAt);
    }
};

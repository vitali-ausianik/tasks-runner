'use strict';

let db = require('./lib/db'),
    co = require('co'),
    uuid = require('uuid');

module.exports = {
    /**
     * Connect to mongo
     * @param {string} url - url to mongo db
     */
    connect: function* (url) {
        yield db.connect(url);
    },

    /**
     * Force close connection to mongo
     * Usually you don't need it
     */
    close: function* () {
        yield db.close();
    },

    /**
     * @param {object} query
     * @returns {*}
     */
    remove: db.remove,

    /**
     * @param {string} name - task name
     * @param {*} data - task data, will be passed into task as an argument
     * @param {Object} [options]
     * @param {string} [options.taskId] - by default uuid will be generated
     * @param {string} [options.group] - null by default
     * @param {Date}   [options.startAt] - in UTC (current date by default - will be executed immediately)
     * @param {number} [options.repeatEvery] - in seconds, (0 by default - disabled)
     */
    schedule: function* (name, data, options) {
        let _options;

        if (!name || 'string' !== typeof name) {
            throw new Error('Name should be of type "string", found "' + typeof name + '"');
        }

        if (options.taskId && 'string' !== typeof options.taskId) {
            throw new Error('Option "taskId" should be of type "string", found "' + typeof options.taskId + '"');
        }

        _options = Object.assign({
            taskId: uuid.v4(),
            group: null,
            startAt: new Date(),
            repeatEvery: 0
        }, options);

        // Don't allow to set group for repeatable task
        if (_options.repeatEvery > 0 && _options.group) {
            throw new Error('Can not specify group for repeatable task');
        }

        return yield db.addTask({
            taskId: _options.taskId,
            name: name,
            data: data,
            group: _options.group,
            startAt: _options.startAt,
            repeatEvery: _options.repeatEvery
        });
    },

    /**
     * @param {object} [options]
     * @param {number} [options.scanInterval] - in seconds, 60 by default
     * @param {number} [options.lockInterval] - in seconds, 60 by default
     * @param {function} [options.taskProcessorFactory] - should return task processor by task name, require() by default
     * @returns {null}
     */
    run: function* (options) {
        let task,
            taskProcessor,
            taskResult,
            previousTask,
            tasksPerCycle = 1000,
            taskIndexInCycle = 0,
            _options = Object.assign({
                scanInterval: 60, // seconds
                lockInterval: 60, // seconds
                taskProcessorFactory: require
            }, options);

        // process tasks until it will be finished or until it will process "tasksPerCycle" items
        do {
            previousTask = null;
            taskIndexInCycle++;
            task = yield db.findTaskToProcess(_options.lockInterval);

            if ( !task ) {
                // there are no tasks in queue
                console.log('task-runner: there are no tasks in queue. Rescan in ' + _options.scanInterval + ' seconds.');
                break;
            }

            if ( task.group ) {
                previousTask = yield db.findPreviousTask(task);
                // check if previous task still was not processed - it means that current task is blocked by previous one
                if ( previousTask && previousTask.processedAt === null ) {
                    // if previousTask' startAt value greater than startAt of current task - reschedule current task on new date
                    if (previousTask.startAt.getTime() > task.startAt.getTime()) {
                        yield db.rescheduleTask(task.taskId, new Date(previousTask.startAt.getTime() + 1000));
                    }
                    continue;
                }
            }

            try {
                let previousTaskResult = previousTask ? previousTask.result : null;
                taskProcessor = _options.taskProcessorFactory(task.name);
                // use taskProcessor as is if it is a function
                // in other case execute taskProcessor.run()
                if ('function' === typeof taskProcessor) {
                    taskResult = yield taskProcessor(task.data, previousTaskResult);

                } else {
                    taskResult = yield taskProcessor.run(task.data, previousTaskResult);
                }

            } catch (err) {
                // something goes wrong with or within task processor - mark task as failed
                console.log(err);
                yield db.markTaskFailed(task.taskId, err.message);

                // reschedule task in (retries+1) minutes
                let delta = (task.retries + 1) * 60 * 1000,
                    scheduleAt = new Date(Date.now() + delta);

                yield db.rescheduleTask(task.taskId, scheduleAt);
                continue;
            }

            if ( task.repeatEvery > 0 ) {
                // task is repeatable and should be rescheduled
                let scheduleAt = new Date(Date.now() + (task.repeatEvery * 1000));
                yield db.rescheduleTask(task.taskId, scheduleAt);

            } else {
                // task is not repeatable, finish it and save its result
                yield db.markTaskProcessed(task.taskId, taskResult);
            }

        } while (taskIndexInCycle < tasksPerCycle);

        // schedule new scan
        setTimeout(co.wrap(this.run.bind(this, _options)), _options.scanInterval * 1000);
    }
};

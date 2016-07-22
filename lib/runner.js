'use strict';

let co = require('co'),
    db = require('./db'),
    config = require('./config'),
    scheduler = require('./scheduler');

/**
 * @class Runner
 */
class Runner {
    /**
     * @param {object} [options]
     * @param {number} [options.scanInterval] - in seconds, 60 by default
     * @param {number} [options.lockInterval] - in seconds, 60 by default
     * @param {function} [options.taskProcessorFactory] - should return task processor by task name, require() by default
     * @param {number} [options.tasksPerScanning] - count of tasks that should be picked per every scanning. By default 1000
     * @param {string} [options.collection] - "tasks" by default
     */
    constructor(options) {
        this._options = Object.assign({
            scanInterval: 60, // seconds
            lockInterval: 60, // seconds
            groupInterval: 5, // seconds
            taskProcessorFactory: require,
            tasksPerScanning: 1000, // tasks that will be executed per every scanning iteration.
            collection: config.collection
        }, options);

        // true if any scanning iteration is in progress
        this.isInProgress = false;

        // graceful shutdown in progress
        this.isStopping = false;

        // runner is stopped
        this.isStopped = false;

        // timer of new scanning iteration
        this.timer = null;
    }

    /**
     * Start scanning iteration based on options, provided via constructor
     * At the end iteration will be rescheduled in this._options.scanInterval seconds
     */
    * start() {
        try {
            yield this.scan();

        } catch (err) {
            // something goes wrong, probably missed connection to mongo - log error
            config.logger.error('Something goes wrong.', err.stack);
        }

        if (this.isStopped) {
            // graceful shutdown
            return;
        }

        // schedule new scanning
        config.logger.debug('Finished scanning iteration, rescan in ' + this._options.scanInterval + ' seconds.');
        this.timer = setTimeout(co.wrap(this.start.bind(this)), this._options.scanInterval * 1000);
    }

    /**
     * Graceful shutdown
     */
    stop() {
        this.isStopping = true;

        if (this.timer) {
            clearTimeout(this.timer);
        }

        if (!this.isInProgress) {
            this.isStopped = true;
        }
    }

    /**
     * Makes one scanning iteration
     */
    * scan() {
        let task,
            iterationIndex = 0;

        this.isInProgress = true;

        // process tasks until it will be finished or until it will process "tasksPerScanning" items
        do {
            iterationIndex++;
            task = yield db.findTaskToProcess(this._options.collection, this._options.lockInterval);

            if ( !task ) {
                // there are no tasks in queue
                break;
            }

            yield this.processTask(task);

        } while (!this.isStopping && iterationIndex < this._options.tasksPerScanning);

        this.isInProgress = false;

        // graceful shutdown
        if (this.isStopping) {
            this.isStopped = true;
        }
    }

    /**
     * Process one task
     * @param {Object} task
     */
    * processTask(task) {
        let previousTask, taskResult;

        config.logger.debug('Start processing of task "' + task.name + '" (' + task.taskId + ')');

        if ( task.group ) {
            previousTask = yield db.findPreviousTask(this._options.collection, task);
            // check if previous task still was not processed - it means that current task is blocked by previous one
            if ( previousTask && previousTask.processedAt === null ) {
                let logInfo = {
                        taskId: task.taskId,
                        blockedBy: previousTask.taskId
                    };

                config.logger.debug('Task is blocked: ' + JSON.stringify(logInfo));

                // reschedule task in 5 seconds using max date from task date and current time
                let curDate = new Date().getTime(),
                    taskDate = previousTask.startAt.getTime(),
                    maxDate = taskDate > curDate ? taskDate : curDate;

                yield db.rescheduleTask(this._options.collection, task.taskId, new Date(maxDate + this._options.groupInterval * 1000), true);

                return;
            }
        }

        try {
            let previousTaskResult = previousTask ? previousTask.result : null,
                extendedInfo = {
                    failedAt: task.failedAt,  // date of previous error
                    errorMsg: task.errorMsg,  // message of previous error
                    retries: task.retries,    // count of failed executions
                    createdAt: task.createdAt // creation date of task
                };

            let taskProcessor = this._options.taskProcessorFactory(task.name);
            // use taskProcessor as is if it is a function
            // in other case execute taskProcessor.run()
            if ( 'function' === typeof taskProcessor ) {
                taskResult = yield taskProcessor(task.data, previousTaskResult, extendedInfo);

            } else {
                taskResult = yield taskProcessor.run(task.data, previousTaskResult, extendedInfo);
            }

        } catch (err) {
            // something goes wrong with or within task processor - mark task as failed
            // and reschedule it based on its retryStrategy
            config.logger.error('Task was failed: "' + task.name + '" (' + task.taskId + ')"', err.stack);
            yield db.markTaskFailed(this._options.collection, task.taskId, err.message);
            yield scheduler.rescheduleFailedTask(this._options.collection, task);
            return;
        }

        if (taskResult === undefined) {
            taskResult = null;
        }

        config.logger.debug('End processing of task "' + task.name + '" (' + task.taskId + '). Result: ', taskResult);

        if ( task.repeatEvery > 0 ) {
            // task is repeatable and should be rescheduled
            yield scheduler.rescheduleRepeatableTask(this._options.collection, task, taskResult);

        } else {
            // task is not repeatable, finish it and save its result
            yield db.markTaskProcessed(this._options.collection, task.taskId, taskResult);
        }
    }
}

module.exports = Runner;

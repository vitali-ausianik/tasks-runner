'use strict';

let db = require('./db'),
    scheduler = require('./scheduler');

module.exports = {
    /**
     * @param {object} options
     * @param {number} options.scanInterval - in seconds
     * @param {number} options.lockInterval - in seconds
     * @param {function} options.taskProcessorFactory
     * @param {number} options.tasksPerScanning
     * @returns {null}
     */
    run: function* (options) {
        let task,
            taskProcessor,
            taskResult,
            previousTask,
            taskIndexInCycle = 0;

        // process tasks until it will be finished or until it will process "tasksPerCycle" items
        do {
            previousTask = null;
            taskIndexInCycle++;
            task = yield db.findTaskToProcess(options.lockInterval);

            if ( !task ) {
                // there are no tasks in queue
                break;
            }

            if ( task.group ) {
                previousTask = yield db.findPreviousTask(task);
                // check if previous task still was not processed - it means that current task is blocked by previous one
                if ( previousTask && previousTask.processedAt === null ) {
                    // if previousTask' startAt value greater than startAt of current task - reschedule current task on new date
                    if ( previousTask.startAt.getTime() > task.startAt.getTime() ) {
                        yield db.rescheduleTask(task.taskId, new Date(previousTask.startAt.getTime() + 1000));
                    }
                    continue;
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

                taskProcessor = options.taskProcessorFactory(task.name);
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
                console.log(err);
                yield db.markTaskFailed(task.taskId, err.message);
                yield scheduler.rescheduleFailedTask(task);
                continue;
            }

            if ( task.repeatEvery > 0 ) {
                // task is repeatable and should be rescheduled
                yield scheduler.rescheduleRepeatableTask(task);

            } else {
                // task is not repeatable, finish it and save its result
                yield db.markTaskProcessed(task.taskId, taskResult);
            }

        } while (taskIndexInCycle < options.tasksPerScanning);
    }
};

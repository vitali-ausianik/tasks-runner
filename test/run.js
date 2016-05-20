'use strict';

require('mocha');
require('co-mocha');

let taskRunner = require('../index'),
    db = require('../lib/db'),
    sinon = require('sinon'),
    assert = require('assert');

describe('.run', function() {
    before(function* () {
        taskRunner.connect('mongodb://localhost:27017/test');
        yield db.remove({});
    });

    after(function* () {
        yield taskRunner.close();
    });

    afterEach(function* () {
        yield db.remove({});
    });

    it('function present', function() {
        assert.equal('function', typeof taskRunner.run, 'Can not find function .run()');
    });

    it('process task with startAt in the past (should be processed)', function* () {
        let taskNamePassedToProcessor = null,
            task = yield taskRunner.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function(name) {
                taskNamePassedToProcessor = name;
                return {
                    run: function* () {
                        return 'expected';
                    }
                }
            };

        yield taskRunner.run({
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
        assert.equal('test', taskNamePassedToProcessor, 'Task name was not passed to task processor');
        assert.equal('expected', task.result, 'Task result was not stored properly');
    });

    it('process task with startAt in the future (should not be processed)', function* () {
        let task = yield taskRunner.schedule('test', 'task data', { startAt: new Date(Date.now() + 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        yield taskRunner.run({
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process locked task with startAt in the past (should not be processed)', function* () {
        let task = yield taskRunner.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        // lock event
        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date() } } );

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process expired lockedAt task with startAt in the past (should be processed)', function* () {
        let task = yield taskRunner.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        // set expired lock
        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date(Date.now() - 70000) } } );

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
    });

    it('process two tasks within specified group (both should be processed)', function* () {
        let task1 = yield taskRunner.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data', {
                group:   'test'
            }),
            taskProcessorFactory = function () {
                return {
                    run: function*() {}
                }
            };

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
    });

    it('process blocked task with specified group (should not be processed)', function* () {
        let task1 = yield taskRunner.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data', {
                group:   'test'
            }),
            taskProcessorFactory = function () {
                return {
                    run: function*() {}
                }
            };

        // lock first task to make it blocker for second task
        yield db._collection.findOneAndUpdate(
            { taskId: task1.taskId },
            {
                $set: {
                    lockedAt: new Date()
                }
            }
        );

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });

        assert.equal(null, task1.processedAt, 'Task was processed when it shouldn\'t');
        assert.equal(null, task2.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process blocked task with specified group and check that it was rescheduled on new time', function* () {
        let task1 = yield taskRunner.schedule('test', 'task data', {
                startAt: new Date(10000),
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test', 'task data', {
                startAt: new Date(1000),
                group:   'test'
            }),
            taskProcessorFactory = function () {
                return {
                    run: function*() {}
                }
            };

        // lock first task to make it blocker for second task
        yield db._collection.findOneAndUpdate(
            { taskId: task1.taskId },
            {
                $set: {
                    lockedAt: new Date()
                }
            }
        );

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });

        assert.equal(null, task1.processedAt, 'Task was processed when it shouldn\'t');
        assert.equal(null, task2.processedAt, 'Task was processed when it shouldn\'t');
        assert.equal(11000, task2.startAt.getTime(), 'Task was not rescheduled or was rescheduled on wrong time');
    });

    it('process repeatable task and check new scheduled date', function* () {
        let task = yield taskRunner.schedule('test', 'task data', {
                repeatEvery: 100,
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        let newStartAt = Date.now() + 100000;
        // assume infelicity of newStartAt is 50 milliseconds
        let startAtDelta = Math.abs(task.startAt.getTime() - newStartAt);
        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds');
        assert.equal(null, task.processedAt, 'Task was finished when it shouldn\'t');
    });

    it('run failed task twice and check "retries" field (should be 2)', function* () {
        let task = yield taskRunner.schedule('test', 'task data', {
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {
                        throw new Error('expected error');
                    }
                }
            };

        yield taskRunner.run({
            taskProcessorFactory: taskProcessorFactory
        });

        // set expired lock and set startAt in the past
        yield db._collection.findOneAndUpdate(
            { taskId: task.taskId },
            {
                $set: {
                    lockedAt: new Date(Date.now() - 2000),
                    startAt: new Date(0)
                }
            }
        );

        yield taskRunner.run({
            lockInterval: 1,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was finished when it shouldn\'t');
        assert.equal(2, task.retries, 'Task was failed twice but "retries" counter is ' + task.retries);
        assert.equal('expected error', task.errorMsg);
    });

    it('run failed task twice and check "startAt" field (should be in two minutes)', function* () {
        let task = yield taskRunner.schedule('test', 'task data', {
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {
                        throw new Error('expected error');
                    }
                }
            };

        yield taskRunner.run({
            taskProcessorFactory: taskProcessorFactory
        });

        // set expired lock and set startAt in the past
        yield db._collection.findOneAndUpdate(
            { taskId: task.taskId },
            {
                $set: {
                    lockedAt: new Date(Date.now() - 2000),
                    startAt: new Date(0)
                }
            }
        );

        yield taskRunner.run({
            lockInterval: 1,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was finished when it shouldn\'t');
        assert.equal(2, task.retries, 'Task was failed twice but "retries" counter is ' + task.retries);
        assert.equal('expected error', task.errorMsg);

        let newStartAt = Date.now() + (60 * 2000);
        // assume infelicity of newStartAt is 50 milliseconds
        let startAtDelta = Math.abs(task.startAt.getTime() - newStartAt);
        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('skip task that is blocked by another', function* () {
        // task2 will be skipped because of not processed task1, task3 should be processed
        let task1 = yield taskRunner.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data', {
                group:   'test'
            }),
            task3 = yield taskRunner.schedule('test 3', 'task data', {
            }),
            taskProcessorFactory = function () {
                return {
                    run: function*() {}
                }
            };

        // lock first task to make it blocker for second task
        yield db._collection.findOneAndUpdate(
            { taskId: task1.taskId },
            {
                $set: {
                    lockedAt: new Date()
                }
            }
        );

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.equal(null, task1.processedAt, 'Task was processed when it shouldn\'t');
        assert.equal(null, task2.processedAt, 'Task was processed when it shouldn\'t');
        assert.notEqual(null, task3.processedAt, 'Task was not processed when it should');
    });

    it('test arguments for taskProcessorFactory().run()', function* () {
        let task1ProcessorRunArgs = null,
            task2ProcessorRunArgs = null,
            task3ProcessorRunArgs = null,
            task1 = yield taskRunner.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskRunner.schedule('test 3', 'task data 3', {
                group:   'test'
            }),
            task1Processor = {
                run: function* () {
                    task1ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    return 'expected result of task 1';
                }
            },
            task2Processor = {
                run: function* () {
                    task2ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    return 'expected result of task 2';
                }
            },
            task3Processor = {
                run: function* () {
                    task3ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    return 'expected result of task 3';
                }
            },
            taskProcessorFactory = function (taskName) {
                switch (taskName) {
                    case 'test 1':
                        return task1Processor;

                    case 'test 2':
                        return task2Processor;

                    case 'test 3':
                        return task3Processor;
                }

                throw new Error('Wrong task name');
            };

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task3.processedAt, 'Task was not processed when it should');

        assert.equal(task1ProcessorRunArgs.length, 3);
        assert(task1ProcessorRunArgs[2].createdAt);
        delete task1ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task1ProcessorRunArgs, [
            'task data 1',
            null,
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);

        assert.equal(task2ProcessorRunArgs.length, 3);
        assert(task2ProcessorRunArgs[2].createdAt);
        delete task2ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task2ProcessorRunArgs, [
            'task data 2',
            'expected result of task 1',
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);

        assert.equal(task3ProcessorRunArgs.length, 3);
        assert(task3ProcessorRunArgs[2].createdAt);
        delete task3ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task3ProcessorRunArgs, [
            'task data 3',
            'expected result of task 2',
            { failedAt: null, errorMsg: null, retries: 0 } ]);
    });

    it('test arguments for taskProcessor.run() in case of some failed task in group', function* () {
        let task1ProcessorRunArgs = null,
            task2ProcessorRunArgs = null,
            task3ProcessorRunArgs = null,
            task1 = yield taskRunner.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskRunner.schedule('test 3', 'task data 3', {
                group:   'test'
            }),
            task1Processor = {
                run: function* () {
                    task1ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    throw Error('Some error');
                }
            },
            task2Processor = {
                run: function* () {
                    task2ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    return 'expected result of task 2';
                }
            },
            task3Processor = {
                run: function* () {
                    task3ProcessorRunArgs = Array.prototype.slice.call(arguments);
                    return 'expected result of task 3';
                }
            },
            taskProcessorFactory = function (taskName) {
                switch (taskName) {
                    case 'test 1':
                        return task1Processor;

                    case 'test 2':
                        return task2Processor;

                    case 'test 3':
                        return task3Processor;
                }

                throw new Error('Wrong task name');
            };

        let findTaskToProcessSpy = sinon.spy(db, 'findPreviousTask');

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });
        db.findPreviousTask.restore();

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.equal(null, task1.processedAt, 'Task was processed when it should not');
        assert.equal(null, task2.processedAt, 'Task was processed when it should not');
        assert.equal(null, task3.processedAt, 'Task was processed when it should not');

        assert.equal(task1ProcessorRunArgs.length, 3);
        assert(task1ProcessorRunArgs[2].createdAt);
        delete task1ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task1ProcessorRunArgs, [
            'task data 1',
            null,
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);

        assert.equal(task2ProcessorRunArgs, null); // task was skipped
        assert.equal(task3ProcessorRunArgs, null); // task was skipped
        assert(findTaskToProcessSpy.calledThrice, 'Function was not called when it should');
    });

    it('test taskProcessor as a function', function* () {
        let task1ProcessorRunArgs = null,
            task2ProcessorRunArgs = null,
            task3ProcessorRunArgs = null,
            task1 = yield taskRunner.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskRunner.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskRunner.schedule('test 3', 'task data 3', {
                group:   'test'
            }),
            task1Processor = function* () {
                task1ProcessorRunArgs = Array.prototype.slice.call(arguments);
                return 'expected result of task 1';
            },
            task2Processor = function* () {
                task2ProcessorRunArgs = Array.prototype.slice.call(arguments);
                return 'expected result of task 2';
            },
            task3Processor = function* () {
                task3ProcessorRunArgs = Array.prototype.slice.call(arguments);
                return 'expected result of task 3';
            },
            taskProcessorFactory = function (taskName) {
                switch (taskName) {
                    case 'test 1':
                        return task1Processor;

                    case 'test 2':
                        return task2Processor;

                    case 'test 3':
                        return task3Processor;
                }

                throw new Error('Wrong task name');
            };

        yield taskRunner.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task3.processedAt, 'Task was not processed when it should');

        assert.equal(task1ProcessorRunArgs.length, 3);
        assert(task1ProcessorRunArgs[2].createdAt);
        delete task1ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task1ProcessorRunArgs, [
            'task data 1',
            null,
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);

        assert.equal(task2ProcessorRunArgs.length, 3);
        assert(task2ProcessorRunArgs[2].createdAt);
        delete task2ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task2ProcessorRunArgs, [
            'task data 2',
            'expected result of task 1',
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);

        assert.equal(task3ProcessorRunArgs.length, 3);
        assert(task3ProcessorRunArgs[2].createdAt);
        delete task3ProcessorRunArgs[2].createdAt;
        assert.deepEqual(task3ProcessorRunArgs, [
            'task data 3',
            'expected result of task 2',
            { failedAt: null, errorMsg: null, retries: 0 }
        ]);
    });

    it('test scheduling of scanning with uncaught error', function* () {
        let clock = sinon.useFakeTimers();

        // run with period of 30 seconds
        yield taskRunner.run({ scanInterval: 30 });

        // next iterations throw an error
        let findTaskToProcessStub = sinon.stub(db, 'findTaskToProcess', function* () { throw new Error('expected error'); });

        // move timer forward to trigger iteration
        clock.tick(30000);

        assert(findTaskToProcessStub.called);
        assert.equal(findTaskToProcessStub.callCount, 1);
        db.findTaskToProcess.restore();
        clock.restore();
    });

    it('test scheduling of task with retryStrategy "none"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'none' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 3 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        let newTask = yield db.findTask({ taskId: task.taskId });

        assert.equal(newTask.retries, 4);
        assert.equal(newTask.startAt.getTime(), task.startAt.getTime());
    });

    it('test scheduling of task with retryStrategy "pow1"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow1' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 3 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 4);

        let newStartAt = Date.now() + 4 * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with retryStrategy "pow2"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow2' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 3 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 4);

        let newStartAt = Date.now() + (4 * 4) * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with retryStrategy "pow3"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow3' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 3 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 4);

        let newStartAt = Date.now() + (4 * 4 * 4) * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with retryStrategy "5m"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: '5m' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 8 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 9);

        let newStartAt = Date.now() + 4 * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with retryStrategy "5h"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: '5h' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 8 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 9);

        let newStartAt = Date.now() + (5 * 60) * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with retryStrategy "5d"', function* () {
        let task = yield taskRunner.schedule('test', 'test data', { retryStrategy: '5d' }),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 8 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 9);

        let newStartAt = Date.now() + (5 * 60) * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });

    it('test scheduling of task with default retryStrategy (not set)', function* () {
        let task = yield taskRunner.schedule('test', 'test data'),
            taskProcessorFactory = function() {
                return function* () {
                    throw new Error('some error');
                }
            };

        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { retries: 3 }});
        yield taskRunner.run({ scanInterval: 300, taskProcessorFactory: taskProcessorFactory });

        task = yield db.findTask({ taskId: task.taskId });
        assert.equal(task.retries, 4);

        let newStartAt = Date.now() + 4 * 60 * 1000,
            startAtDelta = newStartAt - task.startAt.getTime();

        assert(startAtDelta < 50, 'Difference between old startAt and expected one is ' + startAtDelta + ' milliseconds.');
    });
});

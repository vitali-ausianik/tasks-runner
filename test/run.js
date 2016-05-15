'use strict';

require('mocha');
require('co-mocha');

let taskManager = require('../index'),
    db = require('../lib/db'),
    sinon = require('sinon'),
    assert = require('assert');

describe('.run', function() {
    before(function* () {
        yield taskManager.connect('mongodb://localhost:27017/test');
        yield db.remove({});
    });

    after(function* () {
        yield taskManager.close();
    });

    beforeEach(function* () {
        yield db.remove({});
    });

    it('function present', function() {
        assert.equal('function', typeof taskManager.run, 'Can not find function .run()');
    });

    it('process task with startAt in the past (should be processed)', function* () {
        let taskNamePassedToProcessor = null,
            task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function(name) {
                taskNamePassedToProcessor = name;
                return {
                    run: function* () {
                        return 'expected';
                    }
                }
            };

        yield taskManager.run({
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
        assert.equal('test', taskNamePassedToProcessor, 'Task name was not passed to task processor');
        assert.equal('expected', task.result, 'Task result was not stored properly');
    });

    it('process task with startAt in the future (should not be processed)', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() + 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        yield taskManager.run({
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process locked task with startAt in the past (should not be processed)', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        // lock event
        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date() } } );

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process expired lockedAt task with startAt in the past (should be processed)', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        // set expired lock
        yield db._collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date(Date.now() - 70000) } } );

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
    });

    it('process two tasks within specified group (both should be processed)', function* () {
        let task1 = yield taskManager.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data', {
                group:   'test'
            }),
            taskProcessorFactory = function () {
                return {
                    run: function*() {}
                }
            };

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
    });

    it('process blocked task with specified group (should not be processed)', function* () {
        let task1 = yield taskManager.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data', {
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

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });

        assert.equal(null, task1.processedAt, 'Task was processed when it shouldn\'t');
        assert.equal(null, task2.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process blocked task with specified group and check that it was rescheduled on new time', function* () {
        let task1 = yield taskManager.schedule('test', 'task data', {
                startAt: new Date(10000),
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test', 'task data', {
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

        yield taskManager.run({
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
        let task = yield taskManager.schedule('test', 'task data', {
                repeatEvery: 100,
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {}
                }
            };

        yield taskManager.run({
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
        let task = yield taskManager.schedule('test', 'task data', {
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {
                        throw new Error('expected error');
                    }
                }
            };

        yield taskManager.run({
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

        yield taskManager.run({
            lockInterval: 1,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was finished when it shouldn\'t');
        assert.equal(2, task.retries, 'Task was failed twice but "retries" counter is ' + task.retries);
        assert.equal('expected error', task.errorMsg);
    });

    it('run failed task twice and check "startAt" field (should be in two minutes)', function* () {
        let task = yield taskManager.schedule('test', 'task data', {
                startAt: new Date(0)
            }),
            taskProcessorFactory = function() {
                return {
                    run: function* () {
                        throw new Error('expected error');
                    }
                }
            };

        yield taskManager.run({
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

        yield taskManager.run({
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
        let task1 = yield taskManager.schedule('test 1', 'task data', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data', {
                group:   'test'
            }),
            task3 = yield taskManager.schedule('test 3', 'task data', {
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

        yield taskManager.run({
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
            task1 = yield taskManager.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskManager.schedule('test 3', 'task data 3', {
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

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task3.processedAt, 'Task was not processed when it should');
        assert.deepEqual(task1ProcessorRunArgs, [ 'task data 1', null ]);
        assert.deepEqual(task2ProcessorRunArgs, [ 'task data 2', 'expected result of task 1' ]);
        assert.deepEqual(task3ProcessorRunArgs, [ 'task data 3', 'expected result of task 2' ]);
    });

    it('test arguments for taskProcessor.run() in case of some failed task in group', function* () {
        let task1ProcessorRunArgs = null,
            task2ProcessorRunArgs = null,
            task3ProcessorRunArgs = null,
            task1 = yield taskManager.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskManager.schedule('test 3', 'task data 3', {
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

        yield taskManager.run({
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
        assert.deepEqual(task1ProcessorRunArgs, [ 'task data 1', null ]);
        assert.equal(task2ProcessorRunArgs, null); // task was skipped
        assert.equal(task3ProcessorRunArgs, null); // task was skipped
        assert(findTaskToProcessSpy.calledThrice, 'Function was not called when it should');
    });

    it('test taskProcessor as a function', function* () {
        let task1ProcessorRunArgs = null,
            task2ProcessorRunArgs = null,
            task3ProcessorRunArgs = null,
            task1 = yield taskManager.schedule('test 1', 'task data 1', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test 2', 'task data 2', {
                group:   'test'
            }),
            task3 = yield taskManager.schedule('test 3', 'task data 3', {
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

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task1 = yield db.findTask({ taskId: task1.taskId });
        task2 = yield db.findTask({ taskId: task2.taskId });
        task3 = yield db.findTask({ taskId: task3.taskId });

        assert.notEqual(null, task1.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task2.processedAt, 'Task was not processed when it should');
        assert.notEqual(null, task3.processedAt, 'Task was not processed when it should');
        assert.deepEqual(task1ProcessorRunArgs, [ 'task data 1', null ]);
        assert.deepEqual(task2ProcessorRunArgs, [ 'task data 2', 'expected result of task 1' ]);
        assert.deepEqual(task3ProcessorRunArgs, [ 'task data 3', 'expected result of task 2' ]);
    });
});

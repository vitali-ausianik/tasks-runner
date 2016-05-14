'use strict';

require('mocha');
require('co-mocha');

let taskManager = require('../index'),
    db = require('../lib/db'),
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
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function() {
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
        assert.equal('expected', task.result, 'Task\' result was not stored properly');
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
});

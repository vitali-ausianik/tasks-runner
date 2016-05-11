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

    it('process task with startAt in the past', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function(name) {
                return {
                    run: function* () {}
                }
            };

        yield taskManager.run({
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
    });

    it('process task with startAt in the future', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() + 86400) }),
            taskProcessorFactory = function(name) {
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

    it('process locked task with startAt in the past', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function(name) {
                return {
                    run: function* () {}
                }
            };

        // lock event
        yield db.collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date() } } );

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.equal(null, task.processedAt, 'Task was processed when it shouldn\'t');
    });

    it('process expired lockedAt task with startAt in past', function* () {
        let task = yield taskManager.schedule('test', 'task data', { startAt: new Date(Date.now() - 86400) }),
            taskProcessorFactory = function(name) {
                return {
                    run: function* () {}
                }
            };

        // set expired lock
        yield db.collection.findOneAndUpdate({ taskId: task.taskId }, { $set: { lockedAt: new Date(Date.now() - 70000) } } );

        yield taskManager.run({
            lockInterval: 60,
            taskProcessorFactory: taskProcessorFactory
        });

        task = yield db.findTask({ taskId: task.taskId });

        assert.notEqual(null, task.processedAt, 'Task was not processed when it should');
    });

    it('process two tasks within specified group', function* () {
        let task1 = yield taskManager.schedule('test', 'task data', {
                group:   'test',
                startAt: new Date(Date.now() - 200)
            }),
            task2 = yield taskManager.schedule('test', 'task data', {
                group:   'test',
                startAt: new Date(Date.now() - 100)
            }),
            taskProcessorFactory = function (name) {
                return {
                    run: function*() {
                    }
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

    it('process blocked task with specified group', function* () {
        let task1 = yield taskManager.schedule('test', 'task data', {
                group:   'test'
            }),
            task2 = yield taskManager.schedule('test', 'task data', {
                group:   'test'
            }),
            taskProcessorFactory = function (name) {
                return {
                    run: function*() {}
                }
            };

        // lock first task to make it blocker for second task
        // ensure that its createdAt is less then createdAt of second task
        yield db.collection.findOneAndUpdate(
            { taskId: task1.taskId },
            {
                $set: {
                    lockedAt: new Date(),
                    createdAt: new Date(task1.createdAt.getTime() - 1000)
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

    it('process repeatable task and check new scheduled date', function* () {
        let task = yield taskManager.schedule('test', 'task data', {
                repeatEvery: 100,
                startAt: new Date(0)
            }),
            taskProcessorFactory = function(name) {
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
});

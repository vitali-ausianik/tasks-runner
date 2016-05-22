'use strict';

require('mocha');
require('co-mocha');

let db = require('../lib/db'),
    taskRunner = require('../index'),
    assert = require('assert'),
    uuid = require('uuid');

describe('.schedule', function() {
    before(function* () {
        taskRunner.connect('mongodb://localhost:27017/test');
        yield taskRunner.remove({});
    });

    after(function* () {
        yield taskRunner.close();
    });

    afterEach(function* () {
        yield taskRunner.remove({});
    });

    it('function present', function() {
        assert('function', typeof taskRunner.schedule, 'Can not find function .addTask()');
    });

    it('schedule one task', function* () {
        let result = yield taskRunner.schedule('test', 'test data', {});

        assert(result, 'Expect created task');
        assert(result.createdAt instanceof Date, 'Expect field "createdAt" instance of Date');
        assert(result.startAt instanceof Date, 'Expect field "startAt" instance of Date');
        assert('string' === typeof result.taskId, 'Expect field "taskId" of type "string"');

        // delete auto generated fields with unknown values
        delete result.createdAt;
        delete result.startAt;
        delete result.taskId;

        assert.deepEqual(result, {
            name: 'test',
            data: 'test data',
            group: null,
            repeatEvery: 0,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            retries: 0,
            retryStrategy: 'pow1'
        });
    });

    it('schedule one task with specified id', function* () {
        let taskId = uuid.v4(),
            result = yield taskRunner.schedule('test', 'test data', { taskId: taskId});

        assert(result, 'Expect created task');
        assert(result.createdAt instanceof Date, 'Expect field "createdAt" instance of Date');
        assert(result.startAt instanceof Date, 'Expect field "startAt" instance of Date');

        // delete auto generated fields with unknown values
        delete result.createdAt;
        delete result.startAt;

        assert.deepEqual(result, {
            taskId: taskId,
            name: 'test',
            data: 'test data',
            group: null,
            repeatEvery: 0,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            retries: 0,
            retryStrategy: 'pow1'
        });
    });

    it('schedule two tasks with one specified id - error expected', function* () {
        let errorMsg,
            taskId = uuid.v4();

        try {
            yield taskRunner.schedule('test1', 'test data 1', { taskId: taskId });
            yield taskRunner.schedule('test2', 'test data 2', { taskId: taskId });
        } catch (err) {
            errorMsg = err.message;
        }

        assert(errorMsg, 'Expect error for scheduling tasks with the same id');
    });

    it('schedule one task with specified group', function* () {
        let result = yield taskRunner.schedule('test', 'test data', { group: 'expected'});

        assert(result, 'Expect created task');
        assert(result.createdAt instanceof Date, 'Expect field "createdAt" instance of Date');
        assert(result.startAt instanceof Date, 'Expect field "startAt" instance of Date');
        assert('string' === typeof result.taskId, 'Expect field "taskId" of type "string"');

        // delete auto generated fields with unknown values
        delete result.createdAt;
        delete result.startAt;
        delete result.taskId;

        assert.deepEqual(result, {
            name: 'test',
            data: 'test data',
            group: 'expected',
            repeatEvery: 0,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            retries: 0,
            retryStrategy: 'pow1'
        });
    });

    it('schedule one task with specified startAt', function* () {
        let startAt = new Date(Date.UTC(2016, 10, 10)),
            result = yield taskRunner.schedule('test', 'test data', { startAt: startAt });

        assert(result, 'Expect created task');
        assert(result.createdAt instanceof Date, 'Expect field "createdAt" instance of Date');
        assert('string' === typeof result.taskId, 'Expect field "taskId" of type "string"');

        // delete auto generated fields with unknown values
        delete result.createdAt;
        delete result.taskId;

        assert.deepEqual(result, {
            name: 'test',
            data: 'test data',
            group: null,
            repeatEvery: 0,
            startAt: startAt,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            retries: 0,
            retryStrategy: 'pow1'
        });
    });

    it('schedule one task with specified repeatEvery', function* () {
        let result = yield taskRunner.schedule('test', 'test data', { repeatEvery: 60 });

        assert(result, 'Expect created task');
        assert(result.createdAt instanceof Date, 'Expect field "createdAt" instance of Date');
        assert(result.startAt instanceof Date, 'Expect field "startAt" instance of Date');
        assert('string' === typeof result.taskId, 'Expect field "taskId" of type "string"');

        // delete auto generated fields with unknown values
        delete result.createdAt;
        delete result.startAt;
        delete result.taskId;

        assert.deepEqual(result, {
            name: 'test',
            data: 'test data',
            group: null,
            repeatEvery: 60,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            retries: 0,
            retryStrategy: 'pow1'
        });
    });

    it('schedule one task with specified repeatEvery and group - error expected', function* () {
        let errorMsg;

        try {
            yield taskRunner.schedule('test', 'test data', { group: 'test', repeatEvery: 60 });
        } catch (err) {
            errorMsg = err.message;
        }

        assert.equal(errorMsg, 'Can not specify group for repeatable task');
    });

    it('schedule two tasks with one specified name', function* () {
        let result1 = yield taskRunner.schedule('test', 'test data', {}),
            result2 = yield taskRunner.schedule('test', 'test data', {});

        assert(result1, 'Expect created task');
        assert(result2, 'Expect created task');
        assert.equal(result1.name, 'test');
        assert.equal(result2.name, 'test');
    });

    it('schedule two tasks within one group', function* () {
        let group = 'expected',
            result1 = yield taskRunner.schedule('test 1', 'test data 1', { group: group }),
            result2 = yield taskRunner.schedule('test 2', 'test data 2', { group: group });

        assert(result1, 'Expect created task');
        assert(result2, 'Expect created task');
        assert.equal(result1.group, group);
        assert.equal(result2.group, group);
    });

    it('schedule two tasks within two groups', function* () {
        let result1 = yield taskRunner.schedule('test 1', 'test data 1', { group: 'group 1' }),
            result2 = yield taskRunner.schedule('test 2', 'test data 2', { group: 'group 2' });

        assert(result1, 'Expect created task');
        assert(result2, 'Expect created task');
        assert.equal(result1.group, 'group 1');
        assert.equal(result2.group, 'group 2');
    });

    it('test scheduling of task with invalid retryStrategy (string)', function* () {
        let errorMsg;

        try {
            yield taskRunner.schedule('test', 'test data', { retryStrategy: 'not exists' });

        } catch (err) {
            errorMsg = err.message;
        }

        assert.equal(errorMsg, 'Option "retryStrategy" should be matched with one of following patterns ' +
            '[none,powN,Nm,Nh,Nd], where N means any integer. Found "not exists"');
    });

    it('test scheduling of task with invalid retryStrategy (float)', function* () {
        let errorMsg;

        try {
            yield taskRunner.schedule('test', 'test data', { retryStrategy: '5.5m' });

        } catch (err) {
            errorMsg = err.message;
        }

        assert.equal(errorMsg, 'Option "retryStrategy" should be matched with one of following patterns ' +
            '[none,powN,Nm,Nh,Nd], where N means any integer. Found "5.5m"');
    });

    it('schedule tasks with specified retryStrategy', function* () {
        let task1 = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'none' }),
            task2 = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow1' }),
            task3 = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow2' }),
            task4 = yield taskRunner.schedule('test', 'test data', { retryStrategy: 'pow3' }),
            task5 = yield taskRunner.schedule('test', 'test data');

        task1 = yield taskRunner.findTask({ taskId: task1.taskId });
        task2 = yield taskRunner.findTask({ taskId: task2.taskId });
        task3 = yield taskRunner.findTask({ taskId: task3.taskId });
        task4 = yield taskRunner.findTask({ taskId: task4.taskId });
        task5 = yield taskRunner.findTask({ taskId: task5.taskId });

        assert(task1, 'Expect created task');
        assert(task2, 'Expect created task');
        assert(task3, 'Expect created task');
        assert(task4, 'Expect created task');
        assert(task5, 'Expect created task');
        assert.equal(task1.retryStrategy, 'none');
        assert.equal(task2.retryStrategy, 'pow1');
        assert.equal(task3.retryStrategy, 'pow2');
        assert.equal(task4.retryStrategy, 'pow3');
        assert.equal(task5.retryStrategy, 'pow1');
    });
});

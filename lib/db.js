'use strict';

let mongodb = require('mongodb').MongoClient,
    object = {
        url: null,
        collection: null,
        conn: null,
        lockInterval: 60 * 1000 // 60 seconds
    };

/**
 * Connect to mongo and ensure index existence
 * @param {string} url
 */
object.connect = function* (url) {
    object.url = url;
    object.conn = yield mongodb.connect(object.url);
    object.collection = object.conn.collection('tasks');

    console.log('task-manager: connected to ' + object.url);

    object.conn.on('close', function() {
        console.log('task-manager: connection closed');
        object.conn = null;
        object.collection = null;
    });
    yield object.collection.createIndex('taskId', { taskId: 1 }, { unique: true });
    yield object.collection.createIndex('createdAt', { createdAt: 1 });
    yield object.collection.createIndex('processedAt_startAt_lockedAt', { processedAt: 1, startAt: 1, lockedAt: 1 });
};

/**
 * force close connection to mongo
 */
object.close = function* () {
    yield object.conn.close();
};

/**
 * @param {object} query
 * @returns {*}
 */
object.remove = function* (query) {
    return yield object.collection.deleteMany(query);
};

/**
 * Returns created task or null
 *
 * @param {object} task
 * @param {string} task.id - unique
 * @param {string} task.name - task name
 * @param {string} task.data - task data, will be passed into task as an argument
 * @param {string} task.group
 * @param {date}   task.startAt - in UTC, current date by default
 * @param {string} task.repeatEvery - in seconds, 0 by default
 * @returns {null|object}
 */
object.addTask = function* (task) {
    let taskCreated = null,
        _task = Object.assign({
            taskId: task.taskId,
            name: task.name,
            data: task.data,
            group: task.group,
            startAt: task.startAt,
            repeatEvery: task.repeatEvery,
            lockedAt: new Date(0),
            processedAt: null,
            failedAt: null,
            errorMsg: null,
            createdAt: new Date()
        }, task);

    var result = yield object.collection.insertOne(_task);

    if (result.ops) {
        taskCreated = result.ops[0];
        delete taskCreated._id;
    }

    return taskCreated;
};

/**
 * Returns not processed task or null
 * @param {number} lockInterval - in seconds
 * @returns {*}
 */
object.findTaskToProcess = function* (lockInterval) {
    lockInterval = lockInterval * 1000;

    let result = yield object.collection.findOneAndUpdate(
        {
            processedAt: null,
            startAt: { $lt: new Date() },
            lockedAt: { $lt: new Date(Date.now() - lockInterval) }
        },
        {
            $set: { lockedAt: new Date() }
        },
        {
            projection: { _id: 0 },
            sort: { createdAt: 1 }
        }
    );

    return result.value || null;
};

/**
 * Returns count of not processed tasks for provided group with createdAt earlier than provided
 * @returns {*}
 */
object.findTaskBlockersCount = function* (group, createdAt) {
    return yield object.collection.find(
        {
            processedAt: null,
            group: group,
            createdAt: { $lt: createdAt }
        }
    ).count();
};

/**
 * Stores within task "failedAt" field with current date and "errorMsg" field with provided errorMsg
 * @param {string} taskId
 * @param {string} errorMsg
 */
object.markTaskFailed = function* (taskId, errorMsg) {
    let result = yield object.collection.findOneAndUpdate(
        { taskId: taskId },
        {
            $set: {
                failedAt: new Date(),
                errorMsg: errorMsg
            }
        }
    );

    return result.value || null;
};

/**
 * Stores within task "processedAt" field with current date
 * @param {string} taskId
 */
object.markTaskProcessed = function* (taskId) {
    let result = yield object.collection.findOneAndUpdate(
        { taskId: taskId },
        {
            $set: {
                processedAt: new Date()
            }
        }
    );

    return result.value || null;
};

/**
 * Stores task with new "startAt" field
 * @param {string} taskId
 * @param {date} startAt
 */
object.rescheduleTask = function* (taskId, startAt) {
    let result = yield object.collection.findOneAndUpdate(
        { taskId: taskId },
        {
            $set: {
                startAt: startAt
            }
        }
    );

    return result.value || null;
};

/**
 * @param {object} query
 * @returns {*}
 */
object.findTask = function* (query) {
    return yield object.collection.findOne(query);
};

module.exports = object;

'use strict';

let mongodb = require('mongodb').MongoClient;

module.exports = {
    /**
     * url to connect to mongo
     */
    url: null,

    /**
     * link on tasks collection
     */
    _collection: null,

    /**
     * link on mongodb connection
     */
    _conn: null,

    /**
     * Connect to mongo and ensure index existence
     * @param {string} url
     */
    connect: function* (url) {
        this.url = url;
        this._conn = yield mongodb.connect(this.url);
        this._collection = this._conn.collection('tasks');

        console.log('task-runner: connected to ' + this.url);

        yield this._collection.createIndex('taskId', { taskId: 1 }, { unique: true });
        yield this._collection.createIndex('createdAt', { createdAt: 1 });
        yield this._collection.createIndex('processedAt_startAt_lockedAt', { processedAt: 1, startAt: 1, lockedAt: 1 });
    },

    /**
     * force close connection to mongo
     * usually you don't need to do it manually
     */
    close: function* () {
        yield this._conn.close();

        console.log('task-runner: connection closed');
        this._conn = null;
        this._collection = null;
    },

    /**
     * @param {object} query
     * @returns {*}
     */
    remove: function* (query) {
        return yield this._collection.deleteMany(query);
    },

    /**
     * Returns created task or null
     *
     * @param {object} task
     * @param {string} task.taskId - unique
     * @param {string} task.name - task name
     * @param {string} task.data - task data, will be passed into task as an argument
     * @param {string|null} task.group
     * @param {Date}   task.startAt - in UTC, current date by default
     * @param {number} task.repeatEvery - in seconds, 0 by default
     * @returns {null|object}
     */
    addTask: function* (task) {
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
                retries: 0,
                createdAt: new Date()
            }, task);

        var result = yield this._collection.insertOne(_task);

        if (result.ops) {
            taskCreated = result.ops[0];
            delete taskCreated._id;
        }

        return taskCreated;
    },

    /**
     * @param {object} query
     * @returns {*}
     */
    findTask: function* (query) {
        return yield this._collection.findOne(query);
    },

    /**
     * Returns not processed task or null
     * @param {number} lockInterval - in seconds
     * @returns {*}
     */
    findTaskToProcess: function* (lockInterval) {
        lockInterval = lockInterval * 1000;

        let result = yield this._collection.findOneAndUpdate(
            {
                processedAt: null,
                startAt: { $lte: new Date() },
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
    },

    /**
     * Returns not processed task for provided group with createdAt earlier than provided
     * @returns {*}
     */
    findTaskBlocker: function* (group, createdAt) {
        return yield this._collection.find(
            {
                processedAt: null,
                group: group,
                createdAt: { $lt: createdAt }
            },
            {
                sort: { createdAt: -1 }
            }
        ).limit(1).next();
    },

    /**
     * Stores within task "failedAt" field with current date and "errorMsg" field with provided errorMsg
     * @param {string} taskId
     * @param {string} errorMsg
     */
    markTaskFailed: function* (taskId, errorMsg) {
        let result = yield this._collection.findOneAndUpdate(
            { taskId: taskId },
            {
                $set: {
                    failedAt: new Date(),
                    errorMsg: errorMsg
                },
                $inc: { retries: 1 }
            }
        );

        return result.value || null;
    },

    /**
     * Stores within task "processedAt" field with current date
     * @param {string} taskId
     */
    markTaskProcessed: function* (taskId) {
        let result = yield this._collection.findOneAndUpdate(
            { taskId: taskId },
            {
                $set: {
                    processedAt: new Date()
                }
            }
        );

        return result.value || null;
    },

    /**
     * Stores task with new "startAt" field
     * @param {string} taskId
     * @param {Date} startAt
     */
    rescheduleTask: function* (taskId, startAt) {
        let result = yield this._collection.findOneAndUpdate(
            { taskId: taskId },
            {
                $set: {
                    startAt: startAt
                }
            }
        );

        return result.value || null;
    }
};

'use strict';

let mongodb = require('mongodb').MongoClient,
    ObjectID = require('mongodb').ObjectID;

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

        this._conn.on('error', function(err) {
            console.log('tasks-runner: connection error', err.message, err.stack);
        });

        let self = this;
        this._conn.on('close', function() {
            console.log('tasks-runner: connection closed');
            self._conn = null;
            self._collection = null;
        });

        console.log('tasks-runner: connected to ' + this.url);

        yield this._collection.createIndex('taskId', { taskId: 1 }, { unique: true });
        yield this._collection.createIndex('createdAt', { createdAt: 1 });
        yield this._collection.createIndex('group', { group: 1 });
        yield this._collection.createIndex('processedAt_group', { processedAt: 1, group: 1 });
        yield this._collection.createIndex('processedAt_startAt_lockedAt', { processedAt: 1, startAt: 1, lockedAt: 1 });
    },

    /**
     * force close connection to mongo
     * usually you don't need to do it manually
     */
    close: function* () {
        if (this._conn) {
            yield this._conn.close();
        }
    },

    ensureConnection: function* () {
        if (!this.url) {
            throw new Error('tasks-runner: Missed connection to mongodb. Please use .connect() method');
        }

        if (!this._conn) {
            //noinspection JSCheckFunctionSignatures
            yield this.connect(this.url);
        }
    },

    /**
     * @param {object} query
     * @returns {*}
     */
    remove: function* (query) {
        yield this.ensureConnection();
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
     * @param {string} task.retryStrategy
     * @returns {null|object}
     */
    addTask: function* (task) {
        yield this.ensureConnection();
        let taskCreated = null,
            _task = Object.assign({
                taskId: task.taskId,
                name: task.name,
                data: task.data,
                group: task.group,
                startAt: task.startAt,
                repeatEvery: task.repeatEvery,
                retryStrategy: task.retryStrategy,
                lockedAt: new Date(0),
                processedAt: null,
                failedAt: null,
                errorMsg: null,
                retries: 0,
                createdAt: new Date()
            }, task);

        var result = yield this._collection.insertOne(_task);

        if (result.insertedCount === 1) {
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
        yield this.ensureConnection();
        return yield this._collection.findOne(query);
    },

    /**
     * Returns not processed task or null
     * @param {number} lockInterval - in seconds
     * @returns {*}
     */
    findTaskToProcess: function* (lockInterval) {
        yield this.ensureConnection();
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
                sort: { _id: 1 }
            }
        );

        return result.value || null;
    },

    /**
     * Returns previous task in group
     * @param {Object} task
     * @returns {*}
     */
    findPreviousTask: function* (task) {
        yield this.ensureConnection();
        return yield this._collection.find(
            {
                group: task.group,
                _id: { $lt: new ObjectID(task._id) }
            },
            {
                sort: { _id: -1 }
            }
        ).limit(1).next();
    },

    /**
     * Stores within task "failedAt" field with current date and "errorMsg" field with provided errorMsg
     * @param {string} taskId
     * @param {string} errorMsg
     */
    markTaskFailed: function* (taskId, errorMsg) {
        yield this.ensureConnection();
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
     * @param {*} taskResult
     */
    markTaskProcessed: function* (taskId, taskResult) {
        yield this.ensureConnection();
        let result = yield this._collection.findOneAndUpdate(
            { taskId: taskId },
            {
                $set: {
                    processedAt: new Date(),
                    result: taskResult
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
        yield this.ensureConnection();
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

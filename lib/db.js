'use strict';

let mongodb = require('mongodb').MongoClient,
    ObjectID = require('mongodb').ObjectID,
    config = require('./config');

module.exports = {
    /**
     * url to connect to mongo
     */
    url: null,

    /**
     * Array of used collections names. Used to check if indexes were checked for collection.
     *
     * @type {Array}
     * @private
     */
    _collections: [],

    /**
     * link on mongodb connection
     * @private
     */
    _conn: null,

    /**
     * Connect to mongo and ensure index existence
     * @param {string} url
     */
    connect: function* (url) {
        this.url = url;
        let connection = yield mongodb.connect(this.url);
        if (this._conn) {
            // connection already exists, was opened by other task, so just close current one and use opened
            yield connection.close();
            return;
        }

        this._conn = connection;

        this._conn.on('error', function(err) {
            config.logger.error('Connection error.', err.stack);
        });

        let self = this;
        this._conn.on('close', function() {
            config.logger.debug('Connection closed');
            self._conn = null;
            self._collection = null;
        });

        config.logger.debug('Connected to ' + this.url);
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

    /**
     *
     * @param fields
     * @param options
     * @param options.name
     * @returns {*}
     * @private
     */
    _createIndex: function* (collection, fields, options) {
        try {
            return yield this._conn.collection(collection).createIndex(fields, options);

        } catch (err) {
            if (err.name === 'MongoError' && err.code === 85) {
                config.logger.debug(
                    'task-runner: Index with such name but with different options is already exists in database.',
                    'Try to drop it and create new one.',
                    'Original error:',
                    err.message
                );

                yield this._conn.collection(collection).dropIndex(options.name);
                return yield this._conn.collection(collection).createIndex(fields, options);
            }

            // it is not expected error, throw it up
            throw err;
        }
    },

    /**
     * @param {string} collection
     */
    ensureConnection: function* (collection) {
        if (!this.url) {
            throw new Error('Missed connection to mongodb. Please use .connect() method');
        }

        if (!this._conn) {
            //noinspection JSCheckFunctionSignatures
            yield this.connect(this.url);
        }

        // for first usage every collection should be checked for indexes existence
        if (-1 === this._collections.indexOf(collection)) {
            // used: unique identifier
            yield this._createIndex(collection, { taskId: 1 }, { unique: true, name: 'taskId_1' });

            // used: find previous task in group
            yield this._createIndex(collection, { group: 1 }, { sparse: true, name: 'group_1' });

            // used: find next task to process
            yield this._createIndex(
                collection,
                { processedAt: 1, startAt: 1, lockedAt: 1 },
                { name: 'processedAt_1_startAt_1_lockedAt_1' }
            );

            // used: sorting
            yield this._createIndex(collection, { createdAt: 1 }, { name: 'createdAt_1' });

            this._collections.push(collection);
        }
    },

    /**
     * @param {string} collection
     * @param {object} query
     * @returns {*}
     */
    remove: function* (collection, query) {
        yield this.ensureConnection(collection);
        return yield this._conn.collection(collection).deleteMany(query);
    },

    /**
     * Returns created task or null
     *
     * @param {string} collection
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
    addTask: function* (collection, task) {
        yield this.ensureConnection(collection);
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

        var result = yield this._conn.collection(collection).insertOne(_task);

        if (result.insertedCount === 1) {
            taskCreated = result.ops[0];
            delete taskCreated._id;
        }

        return taskCreated;
    },

    /**
     * @param {string} collection
     * @param {object} query
     * @returns {*}
     */
    findTask: function* (collection, query) {
        yield this.ensureConnection(collection);
        return yield this._conn.collection(collection).findOne(query);
    },

    /**
     * Returns not processed task or null
     * @param {string} collection
     * @param {number} lockInterval - in seconds
     * @returns {*}
     */
    findTaskToProcess: function* (collection, lockInterval) {
        yield this.ensureConnection(collection);
        lockInterval = lockInterval * 1000;

        let result = yield this._conn.collection(collection).findOneAndUpdate(
            {
                processedAt: null,
                startAt: { $lte: new Date() },
                lockedAt: { $lt: new Date(Date.now() - lockInterval) }
            },
            {
                $set: { lockedAt: new Date() }
            },
            {
                sort: { createdAt: 1, _id: 1 }
            }
        );

        return result.value || null;
    },

    /**
     * Returns previous task in group
     * @param {string} collection
     * @param {Object} task
     * @returns {*}
     */
    findPreviousTask: function* (collection, task) {
        yield this.ensureConnection(collection);
        return yield this._conn.collection(collection).find(
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
     * @param {string} collection
     * @param {string} taskId
     * @param {string} errorMsg
     */
    markTaskFailed: function* (collection, taskId, errorMsg) {
        yield this.ensureConnection(collection);
        let result = yield this._conn.collection(collection).findOneAndUpdate(
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
     * @param {string} collection
     * @param {string} taskId
     * @param {*} taskResult
     */
    markTaskProcessed: function* (collection, taskId, taskResult) {
        yield this.ensureConnection(collection);
        let result = yield this._conn.collection(collection).findOneAndUpdate(
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
     * @param {string} collection
     * @param {string} taskId
     * @param {Date} startAt
     * @param {boolean} [resetLock]
     * @param {*} [taskResult]
     */
    rescheduleTask: function* (collection, taskId, startAt, resetLock, taskResult) {
        yield this.ensureConnection(collection);

        let update = {
            startAt: startAt,
            result: taskResult
        };

        if (resetLock) {
            update.lockedAt = new Date(0);
            update.retries = 0;
        }

        let result = yield this._conn.collection(collection).findOneAndUpdate(
            { taskId: taskId },
            { $set: update }
        );

        config.logger.debug('Task was rescheduled: ' + JSON.stringify({ taskId: taskId, startAt: startAt }));

        return result.value || null;
    }
};

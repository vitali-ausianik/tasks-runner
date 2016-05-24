'use strict';

let taskRunner = require('../index'); // require('tasks-runner') to use it as dependency of your project

// examples of connection url on http://mongodb.github.io/node-mongodb-native/2.0/tutorials/connecting/
// connect to set of mongos proxies
// let url = 'mongodb://localhost:50000,localhost:50001/myproject';
// connect to a ReplicaSet
// let url = 'mongodb://localhost:27017,localhost:27018/myproject?replicaSet=foo';
// connect to single server
let url = 'mongodb://localhost:27017/test';
// Set url for connection to mongo. Real connection will be created as soon as it will try to execute any query
taskRunner.connect(url);

let taskProcessorFactory = function(taskName) {
    // name contains task.name
    // use it to decide what processor should you return to process this task
    console.log('Providing processor for task with name: ' + taskName);

    switch (taskName) {
        case 'example 1':
            return function* (data, previousTaskResult, extendedTaskInfo) {
                console.log('Passed data during task scheduling: ' + data);
                console.log('Result of previous task of the same group: ' + previousTaskResult);
                console.log('Extended information about current task: ', extendedTaskInfo);
            };

        case 'example 2':
            return {
                someMethod: function() {
                    console.log('do something');
                },
                run: function* (data, previousTaskResult, extendedTaskInfo) {
                    this.someMethod();
                    console.log('Passed data during task scheduling: ' + data);
                    console.log('Result of previous task of the same group: ' + previousTaskResult);
                    console.log('Extended information about current task: ', extendedTaskInfo);
                }
            };

        default:
            throw new Error('Task processor is not defined for task: ' + taskName);
    }
};

taskRunner.run({
    scanInterval: 60, // 60 seconds
    lockInterval: 60, // 60 seconds
    tasksPerScanning: 1000,
    taskProcessorFactory: taskProcessorFactory
}).then(function() {
    console.log('First scanning iteration was finished');
});

process.on('SIGTERM', taskRunner.stop.bind(taskRunner));
process.on('SIGINT', taskRunner.stop.bind(taskRunner));

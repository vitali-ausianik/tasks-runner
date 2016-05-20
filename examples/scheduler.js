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

// schedule task with specified taskId to be sure that nothing else will not create it twice
// in case if task with provided id is already exists - throw an error
// it returns promise so be sure that you use .catch()
taskRunner.schedule(
    'task 1', // task name, will be passed as an argument into taskProcessorFactory()
    { prop1: 'value1', prop2: 'value2' }, // task data, will be passed as an argument into task processor .run()
    { taskId: 'some-unique-task-id' }
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

// schedule task and catch error
taskRunner.schedule(
    'task name 2', // task name, will be passed as an argument into taskProcessorFactory()
    { prop1: 'value1', prop2: 'value2' }, // task data, will be passed as an argument into task processor .run()
    { taskId: 'some-unique-task-id' }
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

// schedule task for specified date
taskRunner.schedule(
    'task name 3',
    { prop1: 'value1', prop2: 'value2' },
    { startAt: new Date(Date.UTC(2020, 2, 10, 4)) } // 4am, Feb 10, 2020
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

// schedule task with specified group,
// so it will process tasks within same group one by one in order like it was scheduled
taskRunner.schedule(
    'task name 4',
    { prop1: 'value1', prop2: 'value2' },
    { group: 'awesome-group' }
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

// schedule repeatable task
taskRunner.schedule(
    'task name 5',
    { prop1: 'value1', prop2: 'value2' },
    { repeatEvery: 60 * 60 } // repeat task once per hour (3600 seconds)
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

// schedule repeatable task for 5pm on daily basis since Feb 25, 2020
taskRunner.schedule(
    'task name 6',
    { prop1: 'value1', prop2: 'value2' },
    {
        startAt: new Date(Date.UTC(2020, 2, 25, 17, 0, 0, 0)), // 5pm, Feb 25, 2020
        repeatEvery: 60 * 60 * 24 // repeat task in daily basis (86400 seconds)
    }
).catch(function(err) {
    console.log('Something goes wrong: ' + err.message);
}).then(function(scheduledTask) {
    console.log('Scheduled task: ' + scheduledTask.name);
});

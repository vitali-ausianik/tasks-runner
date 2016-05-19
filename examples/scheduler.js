'use strict';

let taskRunner = require('../index'), // require('tasks-runner') to use it as dependency of your project
    co = require('co');

co(function* () {
    'use strict';
    try {
        // examples of connection url on http://mongodb.github.io/node-mongodb-native/2.0/tutorials/connecting/
        // connect to set of mongos proxies
        // let url = 'mongodb://localhost:50000,localhost:50001/myproject';
        // connect to a ReplicaSet
        // let url = 'mongodb://localhost:27017,localhost:27018/myproject?replicaSet=foo';
        // connect to single server
        let url = 'mongodb://localhost:27017/test';
        yield taskRunner.connect(url);

        // schedule task with specified taskId to be sure that nothing else will not create it twice
        yield taskRunner.schedule(
            'task name', // task name, will be passed as an argument into taskProcessorFactory()
            { prop1: 'value1', prop2: 'value2' }, // task data, will be passed as an argument into task processor .run()
            { taskId: 'some-unique-task-id' }
        );

        // schedule task for specified date
        yield taskRunner.schedule(
            'task name',
            { prop1: 'value1', prop2: 'value2' },
            { startAt: new Date(Date.UTC(2020, 2, 10, 4)) } // 4am, Feb 10, 2020
        );

        // schedule task with specified group,
        // so it will process tasks within same group one by one in order like it was scheduled
        yield taskRunner.schedule(
            'task name',
            { prop1: 'value1', prop2: 'value2' },
            { group: 'awesome-group' }
        );

        // schedule repeatable task
        yield taskRunner.schedule(
            'task name',
            { prop1: 'value1', prop2: 'value2' },
            { repeatEvery: 60 * 60 } // repeat task once per hour (3600 seconds)
        );

        // schedule repeatable task for 5pm on daily basis since Feb 25, 2020
        yield taskRunner.schedule(
            'task name',
            { prop1: 'value1', prop2: 'value2' },
            {
                startAt: new Date(Date.UTC(2020, 2, 25, 17, 0, 0, 0)), // 5pm, Feb 25, 2020
                repeatEvery: 60 * 60 * 24 // repeat task in daily basis (86400 seconds)
            }
        );

    } catch(err) {
        console.log(err);
    }
});

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

        let myFactory = function(name) {
            // name contains task.name
            // use it to decide what processor should you return to process this task
            console.log('Task name: ' + name);
            return {
                run: function* (data, previousTaskResult) {
                    // process task here
                    // data contains task.data
                    // previousTaskResult contains task.result of previous task in group or null
                    // throw Error in case if task shouldn't be marked as successful
                    console.log('Passed data: ' + data);
                    console.log('Result of previous task in group: ' + previousTaskResult);
                }
            }
        };

        yield taskRunner.run({
            scanInterval: 60, // 60 seconds
            lockInterval: 60, // 60 seconds
            taskProcessorFactory: myFactory
        });

    } catch(err) {
        console.log(err);
    }
});

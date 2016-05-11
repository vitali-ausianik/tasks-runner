'use strict';

let tm = require('../index'),
    co = require('co');

co(function* () {
    'use strict';
    try {
        yield tm.connect('mongodb://localhost:27017/test');
        yield tm.run({
            scanInterval: 10
        });
    } catch(err) {
        console.log(err);
    }
});

'use strict';

require('mocha');
let taskManager = require('../index'),
    assert = require('assert');

describe('.remove', function() {
    it('function present', function() {
        assert.equal('function', typeof taskManager.remove, 'Can not find function .remove()');
    });
});

'use strict';

require('mocha');
let taskRunner = require('../index'),
    assert = require('assert');

describe('.remove', function() {
    it('function present', function() {
        assert.equal('function', typeof taskRunner.remove, 'Can not find function .remove()');
    });
});

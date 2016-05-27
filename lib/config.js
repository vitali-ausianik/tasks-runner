'use strict';

if (!console.debug) {
    console.debug = console.log;
}

module.exports = {
    /**
     * Default collection name.
     */
    collection: 'tasks',

    /**
     * Logger that should be used through package.
     */
    logger: console
};

# Task Runner [![Build Status](https://travis-ci.org/vitali-ausianik/task-runner.svg?branch=master)](https://travis-ci.org/vitali-ausianik/task-runner)
Could resolve following issues:

1. schedule task to be executed at specified time
2. schedule task to be executed with specified period
3. schedule tasks within specified group to be executed one by one in order like it was scheduled

# Installation
```
npm install tasks-runner
```

# How it works
Based on periodical scanning iterations. Every iteration it executes a `tasksPerScanning` count of tasks. New iteration will not
be scheduled until old one finished. When worker pick task - task will be marked with current date and locked.
In another words - this task will be excluded from queue, so nobody will touch it. It could be picked for processing again
with following requirements: task is not still processed and previous task lock was more than `lockInterval` seconds before.
So you need to be sure that every of scheduled tasks will be finished in `lockInterval` seconds.

Task will be marked as failed if it thrown any error and as processed in others cases.

# Task processor
Task processor could be a function or an object with implemented `.run()` method.
Please create it as function-generator or regular function which returns Promise.
Every task processor receives three arguments:

1. Data that it needs to process.
2. Result of previous task in case if both tasks are members of same group. In other cases it passes null.
3. Extended information with recent error (in case of failed previous processing) and creation date of task.

Please see code sample below for details (it is how task processor will be executed by task runner):
```
extendedInfo = {
    failedAt: task.failedAt,  // date of previous error
    errorMsg: task.errorMsg,  // message of previous error
    retries: task.retries,    // count of failed executions
    createdAt: task.createdAt // creation date of task
};
taskResult = yield taskProcessor(task.data, previousTaskResult, extendedInfo);
```

# How to use group of tasks
It is possible to assign a couple of tasks into same group.
In this case these tasks will be executed in order like it was scheduled.
If some task in group will be failed by some reason - others tasks will be postponed.
The result of previous task will be passed to current one as an second argument.
Also every task processor will receive extended information about task as an third argument.

# API
### .connect(url)
Returns Promise. Promise will be resolved as soon as connection will be created. We suggest you to not wait its resolving because it will do it as soon as any query will need it.

[See examples](examples/)

### .schedule(name, data, options)
Returns Promise. Passes scheduled task to resolver.
Schedules task but does not execute it. For execution you will need [worker](examples/worker.js)

Parameter             | Type   | Required | Description
--------------------- | ------ | -------- | -----------
name                  | string | required | Task name, will be passed as an argument into taskProcessorFactory
data                  | mixed  | required | Task data, will be passed as an rgument into taskProcessor
options               | Object | optional |
options.taskId        | string | optional | Unique identifier, by default uuid will be generated
options.startAt       | Date   | optional | Defining when task should be executed. By default - current date.
options.repeatEvery   | number | optional | Period of repeatable task in seconds. By default - 0 (disabled)
options.group         | string | optional | Group of task. By default - null (disabled)
options.retryStrategy | string | optional | In what time task should be rescheduled in case of any error. By default it uses "pow1". Value should be matched with following patterns (N - any integer):<ul><li><b>none</b> - don't reschedule it, task will be run in every scanning iteration</li><li><b>powN</b> - retries count with pow N</li><li><b>Nm</b> - in N minutes</li><li><b>Nh</b> - in N hours</li><li><b>Nd</b> - in N days</li></ul>

[See examples](examples/scheduler.js)

### .run(options)
Returns Promise which will be resolved after first scanning iteration.
Executes scheduled tasks.

Parameter                    | Type     | Required | Description
---------------------------- | -------- | -------- | -----------
options                      | Object   | required |
options.scanInterval         | number   | optional | Period of scanning for ready to execute tasks, in seconds. By default - 60 seconds.
options.lockInterval         | number   | optional | Max time for task to be finished or failed, in seconds. By default - 60 seconds.
options.taskProcessorFactory | function | optional | Should return task processor for provided task name (via first argument). Task processor could be a function or an object with .run() method. As an argument task processor receives task data. Also it can receive result of previous task execution in case if both tasks are members of the same group.
options.tasksPerScanning     | number   | optional | Count of tasks that should be executed per scanning iteration. By default - 1000. Iteration should be finished in scanInterval seconds.

[See examples](examples/worker.js)

### .close()
Returns Promise or undefined (in case if connection is not exists).
Force closing connection to mongo. Usually you don't need to do it manually, but probably you will need it for some tests.

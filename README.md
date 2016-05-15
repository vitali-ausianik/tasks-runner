Task Runner [![Build Status](https://travis-ci.com/vitali-ausianik/task-runner.svg?token=LqnKjCz4apWQtEE5ynMc&branch=master)](https://travis-ci.com/vitali-ausianik/task-runner)
============
Could resolve following issues:

1. schedule task to be executed at specified time
2. schedule task to be executed with specified period
3. schedule tasks within specified group to be executed one by one in order like it was scheduled

How to use group of tasks
=========================
It is possible to assign a couple of tasks into same group.
In this case these tasks will be executed in order like it was scheduled.
If some task in group will be failed by some reason - others tasks will be postponed.
The result of previous task will be passed to current one as an second argument of its .run() method.

[See examples](examples/scheduler.js)

# promec-watcher
A Watcher whose task is to watch a directory and once a change detected, launch a Comet-indexer job in kubernetes using Job API.
It keeps track of running jobs and see if there is any pending pod, then it backs off and wait for it to get started before scheduling new jobs.

## Hinted Handoff Tasks

When a node is unreachable from a coordinator, a hint file is created on disk. Hinted handoff tasks will replay those files and remove them from disk once the hint has been replayed.
This is an optimization to avoid needing to run a repair whenever a node is down. Excessive hints can lead to poor cluster performance.

### Live Hints
The TP stats shows how many hints are replaying and how many have completed. These values reset if a node has been restarted

{{liveHints}}

### Log Hint Dispatcher Message Metrics
The logs contain an entry for each time that a hint file has been dispatched to a node, due to the log file durations, this can be greater or less than the JMX metrics.

{{logHints}}

{{#hitMessageLimit}}
We found more than 1,000,000 log messages of hint dispatching within the logs.
{{/hitMessageLimit}}
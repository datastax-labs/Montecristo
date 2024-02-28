## Logging

By default {{software}} is configured to log to both a rolling file typically at /var/log/cassandra/system.log and standard out (for the process) at the same time. Depending on how Cassandra is started, logging to standard out may be directed to a file that will grow indefinitely without any management of its size. In addition, writing logs to file and to STDOUT will take additional time as the same message is being written twice.

Logging is using the following settings:  

```
{{loggingConfig}}
```

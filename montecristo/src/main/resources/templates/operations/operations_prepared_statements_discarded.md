## Prepared Statements

Each query from an application can be prepared and stored within a prepared statement cache. If an application prepares a statement incorrectly and include literal values, the statement cache floods rapidly with hundreds of versions of the same query.

When the database detects this, {{software}} issues a warning into the logs that it is discarding too many prepared statements. The warning will appear such as:

````
WARN  [ScheduledTasks:1] 2020-11-02 11:20:24,835 QueryProcessor.java:148 - 123 prepared statements discarded in the last minute because cache limit reached (125 MB)
````

The query cache can be inspected by querying the `system.prepared_statements` table with CQL, queries that appear with literal values are not prepared correctly and should be corrected within the application code.

{{#warningsByDateTable.empty}}
No prepared statement discard warnings detected.
{{/warningsByDateTable.empty}}

{{^warningsByDateTable.isEmpty}}
{{^hitMessageLimit}}
There were {{numWarnings}} warnings within the logs.
{{/hitMessageLimit}}

Warnings by Date for the last 14 dates:

{{warningsByDateTable}}

{{/warningsByDateTable.isEmpty}}

{{#hitMessageLimit}}
We found more than {{numWarnings}} messages of discarded prepared statements within the logs.
{{/hitMessageLimit}}
## Materialized Views

Materialized Views were added in Cassandra 3.0 but have not yet reached a point of stability, and have recently been marked as experimental.  They have numerous issues related to data integrity and lack tooling to validate or repair the data in the view against the base table.

Materialized views also have a tendency to result in extremely large partitions if not very carefully planned out.  They must be approached with the same data modeling best practices as any other table.

Nodes down longer than the hint window can result in data becoming out of sync, and repairing materialized views takes orders of magnitude longer than repairing a normal table.

We do not recommend using materialized views at this time.

{{mvtable}}
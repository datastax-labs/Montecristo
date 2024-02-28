## OpsCenter

DataStax OpsCenter is an easy-to-use visual management and monitoring solution for DataStax Enterprise (DSE), the always-on data layer for real-time applications. With OpsCenter, you can quickly provision, upgrade, monitor, backup/restore, and manage your DSE clusters with little to no expertise.

OpsCenter itself uses DataStax Enterprise to store the telemetry data and aggregated data that is used by the web interface.

OpsCenter should be configured to store the metric data in a different cluster to the one it is monitoring - if stored on the same cluster it contends for I/O and other resources.

https://docs.datastax.com/en/opscenter/6.8/opsc/configure/opscStoringCollectionDataDifferentCluster_t.html

The creation of a DSE Enterprise cluster purely for the purpose of storing OpsCenter metric data does not require additional DSE licenses.

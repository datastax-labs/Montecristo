## Schema Versions

In a peer-to-peer distributed system such as {{software}}, schema disagreement is caused by schema changes. When a series of schema updates are made, announcements of these can lead to a high volume of traffic over
{{software}}’s messaging service.

In large clusters, frequent schema updates can cause problems. If nodes are down during a period of frequent concurrent schema changes, then the disagreement can remain unresolved in the cluster.

Ongoing schema disagreement can lead to loss of data. It can cause degraded availability for applications that expect or depend upon a particular schema version which some nodes are not carrying.

Our discovery process deals only with a snapshot of the cluster's state, and if data is collected at different times we commonly see multiple schemas. However disagreement on schemas amongst nodes and slow schema propagation can be symptomatic of deeper problems in the cluster. If they are observed frequently this should be investigated.

{{#agreement}}
The schema versions are all in agreement.
{{/agreement}}
{{^agreement}}
{{schemaVersions}}
{{/agreement}}
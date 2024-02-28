## Authentication


{{authSettings}}

Each time a query is run, the client must be authenticated and (if this is successful) have its permissions checked. This can be an expensive process, and by caching permissions for longer, we can avoid repeated reads from disk for the system_auth tables.

This is set conservatively by default because when the validity period for credentials is extended, they will be checked for revocation less frequently. We find that credentials are rarely revoked in most
{{software}} implementations, but if this occurs frequently in your system it may be a factor to consider.

---

_**Consideration**: The {{software}} superuser uses QUORUM queries for authentication while other (super)users use ONE. A new superuser should be created for ops access and the cassandra user should be disabled.
Apps and services should never access the database using a superuser._

{{#noAuth}}

---

**Noted For Reference**: Enabling authentication and authorization will require additional changes including increasing the replication factor of the system_auth keyspace, adjusting cache timeouts, and creating roles and permissions. We can provide a runbook as well as automation to assist with these changes.

---
{{/noAuth}}

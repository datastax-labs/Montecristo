## GC grace seconds

GC Grace Seconds is used by {{software}} to control when tombstones (deletion markers) can be purged from disk and when they should no longer be replicated in repair. It is controlled by the table property `gc_grace_seconds`.

The default setting is 864000 (10 days). When using the default, operators are required to run a repair at least every 10 days.


{{gc_grace_values}}



## Dropped Hints

The Hinted Handoff system only stores hints if a node has been down for less than `max_hint_window_in_ms` which defaults to 10800000 or 3 hours. When nodes are down for longer than the timeout, hints are no longer collected and the node will not receive missed writes. When the down node returns it will not receive hints for writes that occurred after the `max_hint_window_in_ms` expired.

Total Dropped Hints: {{totalDropped}}

{{^droppedReport.isEmpty}}
Dropped Hints by Node:

{{droppedReport}}
{{/droppedReport.isEmpty}}


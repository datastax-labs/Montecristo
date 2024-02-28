## Replication strategy


* **SimpleStrategy**: A basic replication strategy which places replicas by moving around the ring.  Does not take into account racks or data centers.  This should never be used in production, it is always wrong.
* **NetworkTopologyStrategy**: This strategy adds to simple strategy by enabling rack awareness and DC awareness.  This is the correct strategy to use in production.

{{keyspaces}}

{{#keyspacesToSwitch.empty}}
We have no recommendations at this time.
{{/keyspacesToSwitch.empty}}

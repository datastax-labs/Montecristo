## NTP

The NTP service ensures all nodes within the {{software}} ring always have their clocks synced to allow for reliable timestamp resolution.

When using a driver for {{software}} that supports CQL 3.0+, the new default is to have client applications generate client-side timestamps to be used for mutation resolution. Because of this change in functionality, it is recommended to have NTP installed on all client nodes that issue mutation requests as well as all {{software}} nodes.


```
{{ntp}}
```

---

_**Noted For Reference:** The NTP offset should be inside (-10, 10) ms interval._

---

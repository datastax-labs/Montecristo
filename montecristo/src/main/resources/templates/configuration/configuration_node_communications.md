## Node Communications

Nodes communicate with each other via two connections to every other node. The Command channel is used to send request messages, and the acknowledgement channel is used to send replies. Message passing between nodes is configured by several settings.

The `inter_dc_tcp_nodelay` setting enables and disables the Nagle algorithm when communicating across data centers. By default `inter_dc_tcp_nodelay` is disabled, which enables the Nagle algorithm and reduces the amount of packets sent across the WAN. This is the correct setting to use when data centers are separated by a large lag, or unreliable networks such as the public internet.


{{nagleSetting}}

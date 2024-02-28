## Address Bindings

{{software}} binds to two interfaces, and advertises three addresses to other nodes and clients. The two bound interfaces are used to communicate with other nodes and to communicate to clients. The third address that is advertised to other nodes is a "Broadcast Address" and is used when the node is behind a NAT. Communications are configured as follows:

* Internode (node-to-node) is configured using the `listen_address`.
* Client (client-to-node) is configured using the `rpc_address`.
* Broadcast address is configured using the `broadcast_address`.

{{address-bindings}}


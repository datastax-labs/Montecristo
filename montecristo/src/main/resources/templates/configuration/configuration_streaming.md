## Streaming

Streaming is the process {{software}} uses during repair, rebuild and bootstrapping to transfer sections of SSTables between nodes. Large chunks of files can be efficiently streamed between nodes, avoiding the read and write API. There are several settings that control streaming in the configuration.

The `streaming_socket_timeout_in_ms` setting specifies how long to wait before timing out. The default in versions 1.2 and 2.0 in use is 0, which disables the timeout. With this in place nodes must be restarted to clear failed streaming sessions.

{{streaming}}


## Storage

{{software}} has two distinct locations that can be configured for storage of data and commit log. Each device can have a different scheduler and read ahead configured.

Read ahead is designed for slower spinning disks where latency is the limiting factor.  By reading extra information into the page cache, read ahead trades off some throughput for an improvement in latency.  We set the read ahead quite low (16KB).

The storage devices for the data are configured as follows:

{{storage}}

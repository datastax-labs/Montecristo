## Swap

Swap is used when RAM is full.  Generally speaking, {{software}} performs poorly when using swap space.  

{{^swapReport.isEmpty}}
The nodes in this cluster are using different configuration settings.  This is a full breakdown of the configuration settings:

{{swapReport}}

{{/swapReport.isEmpty}}
{{#swapReport.isEmpty}}
No swap files were detected on the nodes.
{{/swapReport.isEmpty}}


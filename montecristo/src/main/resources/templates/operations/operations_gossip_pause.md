## Log Pause Warnings

{{software}} has a mechanism that checks on itself to see if it has paused for too long. This mechanism helps to prevent the node from creating false positives and marking other nodes down in gossip. If these messages occur at any time other than the node starting up, then the message is a sign of a high load or the underlying OS being paused as can occur on a virtual machine.
 
An example of the message:
````
WARN  [GossipTasks:1] 2020-11-09 11:06:02,475 FailureDetector.java:278 - Not marking nodes down due to local pause of 81905048065 > 5000000000
````

If the number of warnings is relatively low then this is not a direct cause for concern and could be due to local restarts when applying patches or other operational reasons. If the number of warnings seen is elevated, the node will appear offline to other nodes more frequently, and will result in dropped operations. 

{{countOfWarnings}}

{{localPauseMessagesTable}}

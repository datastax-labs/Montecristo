## Disk Failure Policies

The disk failure policy controls how {{software}} reacts to issues when accessing the data directories disks.  
It is configured as follows:  

{{diskFailurePolicySetting}}
  
The commit failure policy controls how {{software}} reacts to issues when accessing the commit log disks.  
It is configured as follows:

{{commitFailurePolicySetting}}

A value of **stop** can be used for both the `disk_failure_policy` and `commit_failure_policy` settings where the
{{software}} service is set up to auto-restart. In this case, appropriate monitoring must be in place to allow detection of nodes in a zombie state. Otherwise, where auto-restart is unused a value of **die** should be used for both the `disk_failure_policy` and `commit_failure_policy` settings. In this case, using a setting other than **die** would leave the
{{software}} process running which may create confusion or even hide outages.


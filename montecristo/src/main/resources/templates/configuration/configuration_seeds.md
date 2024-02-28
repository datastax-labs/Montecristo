## Seeds
  
Seeds are used as "well known" nodes in the cluster. When a new node starts, it uses the seed list to find the other nodes in the cluster. They also function as "super nodes" in the Gossip protocol, where they receive gossip updates more frequently than other nodes. The list of seeds nodes is configured using the "seeds" configuration setting.  
  
The recommended approach is to use the same seed list for all nodes in a cluster and for it to contain three nodes from each DC.  
  
{{seedConfiguration}}


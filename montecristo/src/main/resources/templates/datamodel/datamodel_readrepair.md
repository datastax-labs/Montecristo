## Read repair

Read Repair is a process that repairs inconsistencies between replicas during the read path.
It functions in two modes:

* **Blocking Read Repair** checks the results from replicas when a Consistency Level above ONE is used
* **Non Blocking Read Repair** works in the background of a read to compare the data from more nodes than are required to meet the Consistency Level. 

Non Blocking Read Repair is controlled by two table properties: `read_repair_chance` and `dclocal_read_repair_chance`.

The current defaults for these values are:

```
dclocal_read_repair_chance: 0.1
read_repair_chance: 0.0
```

{{repairTable}}

Using read repair can add significant overhead without offering any guarantees. We recommend using scheduled repairs to manage entropy, as the read repair chance is not intended as a reliable approach to repairing all data. More information on the topic can be read in [this blog post](https://thelastpickle.com/blog/2021/01/12/get_rid_of_repair_repair_chance.html). Reach out to DataStax Services for further help on scheduled repairs.
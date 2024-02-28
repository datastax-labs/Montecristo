## CQL Collection types

{{software}} supports three data types to store multiple values inside a single column:

* **Maps**: These are a key/value pair combination. Inserting a new element is append only i.e. no read before write required to insert.
* **Sets**: Contains unique elements.  Adding an element to a set results in the same set if the value already existed in the set. Inserting a new element is append only i.e. no read before write required to insert.
* **Lists**: Lists are an ordered collection which can contain many of the same element. They support appending and prepending. Some operations require a read before write, or are non-idempotent. Append and prepend operations are one such example. Whilst both are purely write (no read before write required), they are non-idempotent. This property makes the behaviour or lists erratic when retrying the operation after a timeout. We recommend using a set instead of a list whenever possible.

Maps and sets are [Conflict-free replicated data types](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type), or CRDTs, meaning simultaneous operations on the collections on different servers will converge to the same value on both servers. For instance, if a set contains elements A and B, removing A on node server and B on another server will result in both servers thinking the set contains no elements. In contrast, a list lacks this property, as removing an element from the head of a list on one server while simultaneously adding an element to the head of the list on another server will result in a conflict.

Collection types can be:

* **frozen**: The whole content of the collection is serialized and stored as one value. This type solves some of the problems described below, however updates to individual elements of a frozen collection are disallowed. A collection becomes frozen in a schema when the frozen keyword is specified in the field definition of the table.
* **non-frozen**: The collection is stored as a set of individual elements in separate cells. By default, a collection is non-frozen unless the frozen keyword is specified in the field definition of the table. 


Collection types provide a way to group together multiple values that could apply to a particular field. When using them, the following points should be taken into consideration:

* Additional overhead for keeping metadata of individual elements when using non-frozen collections. This includes a write timestamp and optional TTL. For list type, there is an additional overhead to store the index of the elements for which the UUID is used (16 bytes per element).
* When an insert or full update of a non-frozen collection occurs, a tombstone is created. For example, you can replace the value of a column with another value using a CQL statement similar to: `UPDATE table SET field = new_value ...`. In this case
  {{software}} inserts a tombstone marker to prevent possible overlap with previous data. This occurs even if there was no data that previously existed. If large numbers of updates occur, it will result in large numbers of tombstones which can significantly impact read performance.
* There is an upper bound on the number of elements in collection. For Cassandra 3.0.1 and later: 2 billion. For earlier versions: 65,535. Higher numbers of elements can result in either performance problems when accessing data in non-frozen collections, or exceeding the maximum mutation size limits when using frozen collections. In addition, when reading a column with a collection type, its whole content is returned. A large data transfer such as this could harm performance.
* For non-frozen collections where individual elements were updated after insert, performance can degrade. This is because column updates could be spread between multiple SSTables. To read back the values, all the SSTables that contain the column update need to be read to reconstruct the final column value.
* Once a collection has been declared frozen or non-frozen, there is no way to update the column to switch to the opposite declaration. That is, if you create a schema with a non-frozen collection there is no way to update the column to a frozen collection later on, and vice-versa. You will need to perform a data migration to a newly created table to perform such a change.
* Read repair never propagates tombstones. If a cluster has irregular or no repairs, deleted elements in a collection may be affected. This happens because delete markers (defined by a custom tombstone) are never sent to other replicas. As a result, when using a weak consistency level, read requests may return inconsistent data.

The problems listed above can be mitigated by following several rules:

**Use frozen collections**

Unless there is a necessity to update individual elements, use frozen collections.

**Minimize the number of elements in the collection**

Keep the number of elements in all collection types to the order of dozens with a maximum of several hundreds of elements. Collection column content is read as a whole, this means when a collection has too many elements read problems will occur. This is due to the maximum size of a page being restricted to 256 MB.

Note: When a query returns many rows, it is inefficient to return them as a single response message. Instead, the driver breaks the results into pages which get returned as needed. Applications can control how many rows are included in a single page, but there is a maximal size of the page defined by the native protocol.

**Use append operations for empty maps or sets**

When no previous data exists in a non-frozen set or map, use an append operation for columns when inserting data or performing a full update. This will prevent the creation of tombstones. For example, consider the table:

```
CREATE TABLE test.m1 (
id int PRIMARY KEY,
m map<int, text>
);
```

If either of the following is used:

```
INSERT INTO test.m1(id, m) VALUES (1, {1:'t1', 2:'t2'});
```

or

```
UPDATE test.m1 SET m = {1:'t1', 2:'t2'} WHERE id = 1;
```
 
tombstones will be generated. Instead, use:

```
UPDATE test.m1 SET m = m + {1:'t1', 2:'t2'} WHERE id = 1;
```

which has the same result, but without tombstone generation.

If there is only one column with a collection type in the table, you could model it as an additional clustering column. For example:

```
CREATE TABLE test.m1 (
id int PRIMARY KEY,
m map<int, text>
);
```

This table could be created without the map column. The same approach is used for sets and lists:

```
CREATE TABLE test.m1 (
 id int,
 m_key int,
 m_value text,
 PRIMARY KEY(id, m_key)
);
```

You can select either all values for specific partitions by omitting the condition on `m_key` or selecting only the specific element by providing a full primary key. It’s a bigger advantage over the column with collection type, which is returned as a whole.

List type has additional limitations:

* Setting and removing an element by position and removing occurrences of particular values incur an internal read-before-write.
* Prepend or append operations are non-idempotent.
In case of a failure the state of the list is unknown. Specifically, it is unknown if the operation was able to alter the list when the failure occurred. Retrying the operation may result in duplicate elements; ignoring the failure without a retry may result in loss of data. For more information, see the [List fields](https://docs.datastax.com/en/dse/6.8/cql/cql/cql_using/useInsertList.html) documentation.

If you don’t need to keep elements in specific order or have elements with duplicate values, use set type instead of list type. If you still need to use a column with the list type, consider using a frozen version of it.

{{tables_with_collections}}

{{collections_recs}}
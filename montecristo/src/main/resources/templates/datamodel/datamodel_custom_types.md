## Custom types

{{software}} allows creation of User Defined Types (UDTs). This type allows you to group related information together and use it as a single entity. From a data model analysis point of view, you can apply the same rules as for collections:

* Use frozen UDTs where possible.
* For non-frozen UDTs keep the number of fields to a minimum.

One of the problems with UDTs arises from a schema evolution standpoint. While it is possible to add fields to UDTs, it is impossible to remove them.

UDTs should only be used sparingly and when absolutely necessary. Otherwise, it is preferable to structure the data as regular columns in the table. Alternatively, store the UDT as a text blob and perform serialization and deserialization of UDT data inside the application.

UDTs can be nested inside other UDTs or as elements in the collections. Caution needs to be taken when doing this. If too many elements exist in a collection or there are too many nested UDTs, then the size of the mutation will increase. If the maximum mutation size limit is reached, operations involving the table will fail.

{{types}}

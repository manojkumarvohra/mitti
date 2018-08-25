# mitti
## *A generic way to perform CRUD operations with HBASE or MAPRDB*

-----------------
High Level Design
-----------------

- Using this project one can perform CRUD operations from Java applications with HBase or MaprDB.
- Entity pojo design structure should follow below convention:
  - field names must follow pattern: **_columnfamily_column_**
     - Type could be: String, Short, Integer, Long, Float, Double 

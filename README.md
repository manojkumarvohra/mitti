# mitti
## *A generic way to perform CRUD operations with HBASE or MAPRDB*

-----------------
High Level Design
-----------------

- Using this project one can perform CRUD operations from Java applications with HBase or MaprDB.
- Entity pojo design structure should follow below convention:
  - field names must follow pattern: **_columnfamily_column_**
     - Type could be: _String, Short, Integer, Long, Float, Double_ 
  - if any column family is supposed to take variable number or name of columns (which are not known upfront) so as to follow dynamic schema, then that field has to be defined as a **_Map<String,String>_**
    - Field Naming structure for these would be **_columnfamily_**
    - Entity class MUST be annotated with: @DynamicColumnFamily
        - the field MUST be added to fields array of @DynamicColumnFamily

# mitti
_mitti is a hindi word which means soil. Soil supports variety and large number of families. We start from, fetch from and finally end in soil_
## *A generic way to perform CRUD operations with HBASE or MAPRDB*

-----------------
POJO Design
-----------------

- Using this project one can perform CRUD operations from Java applications with HBase or MaprDB.
- Entity pojo design structure should follow below convention:
  - field names must follow pattern: **_columnfamily_column_**
     - Type could be: _String, Short, Integer, Long, Float, Double_ 
  - if any column family is supposed to take variable number or name of columns (which are not known upfront) so as to follow dynamic schema, then that field has to be defined as a **_Map<String,String>_**
    - Field Naming structure for these would be **_columnfamily_**
    - Entity class MUST be annotated with: _@DynamicColumnFamily_
        - the field MUST be added to **fields** array of _@DynamicColumnFamily_
  - Declare getter/setter methods for all fields
  - Entity POJO MUST implement _KVPersistable_ interface
  - You can define getters starting with **get_** these methods would be ignored by driver
  	- You are also free to define other utility methods if required which dont start with **get** or **set** 
  - Have a look @ class com.mitti.models.SampleEntity

```java
@DynamicColumnFamily(fields = { "varcf" })
public class SampleEntity implements KVPersistable {

	private String row_key;

	/*
	 * ColumnFamily: basic Column: name
	 */
	private String basic_name;

	/*
	 * ColumnFamily: basic Column: age
	 */
	private Integer basic_age;

	/*
	 * ColumnFamily: other Column: entity_score
	 */
	private Float other_entity_score;

	/*
	 * ColumnFamily: varcf Column: this can support variable number of columns by
	 * this declaration. This would help in cases for flexible schema.
	 */
	private Map<String, String> varcf;

	@Override
	public String get_Row_key() {
		return row_key;
	}

	@Override
	public void setRow_key(String row_key) {
		this.row_key = row_key;
	}

	public String getBasic_name() {
		return basic_name;
	}

	public void setBasic_name(String basic_name) {
		this.basic_name = basic_name;
	}

	public Integer getBasic_age() {
		return basic_age;
	}

	public void setBasic_age(Integer basic_age) {
		this.basic_age = basic_age;
	}

	public Float getOther_entity_score() {
		return other_entity_score;
	}

	public void setOther_entity_score(Float other_entity_score) {
		this.other_entity_score = other_entity_score;
	}

	public Map<String, String> getVarcf() {
		return varcf;
	}

	public void setVarcf(Map<String, String> varcf) {
		this.varcf = varcf;
	}
}
```
-------------------
TOOLS VERSIONS USED
-------------------
- JAVA: 1.8
- HBASE: 2.0.0
- HADOOP: 2.6.5

-----------
HOW TO USE?
-----------
- Build the maven project.
- Use the mitti-hbase-0.0.1.jar in your projects as dependency.
- Instantiate HbaseDriver class and use it for CRUD operations.

-----------------
TEST APPLICATION
-----------------
  - Create the entity table before performing CRUD operations:
  ![create table](/src/main/resources/images/create_table.jpg?raw=true "Create Table")
  - Update application.properties under resources as per your cluster
```
hbase.usemaprdb=false
hbase.maprdb.path=maprfs:///mapr/db/warehouse/dev/
hbase.zookeeper.quorum=localhost
hbase.zookeeper.property.clientPort=2181
```
  - Run _App.java_ to test entity creation:
  ![create entity](/src/main/resources/images/tbl_scan.png?raw=true "Create Entity")
  
  - More examples on using the CRUD operations can be seen in **_HbaseDriverTest_**

----------------
DRIVER APIs
----------------
Hbase driver supports following operations:

- addUpdate a single entity
```java
public <T extends KVPersistable> boolean addUpdate(T t, java.lang.String queryTable,                                      java.lang.Class<T> entityClass)
```
- addUpdate a list of entities in one call
```java
public <T extends KVPersistable> boolean addUpdateAll(java.util.List<T> arrT, java.lang.String queryTable, java.lang.Class<T> entityClass)
```
- delete a row from table by rowkey
```java
public boolean deleteById(java.lang.String row_key, java.lang.String queryTable)
```
- delete rows from table based on a Filter
```java
public <T extends KVPersistable> boolean deleteByFilter(java.lang.String queryTable, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter)
```
- delete a selected list of columns under a column family for a given row key
```java
public boolean deleteColumnsById(java.lang.String row_key, java.lang.String queryTable, java.lang.String columnFamily, java.lang.String... columns)
```
- Get all rows from a table
```java
public <T extends KVPersistable> java.util.List<T> query(java.lang.String table, java.lang.Class<T> entityClass)
```
- Get row from a table based on key
```java
public <T extends KVPersistable> T query(java.lang.String row_key, java.lang.String table, java.lang.Class<T> entityClass)
```
- Get rows from a table based on matching filter
```java
public <T extends KVPersistable> java.util.List<T> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter)
```
- Get rows from a table based on matching filter and staring rowkey (inclusive)
```java
public <T extends KVPersistable> java.util.List<T> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter, java.lang.String startRow)
```
- Get row from a table based on key and provided filter
```java
public <T extends KVPersistable> T query(java.lang.String row_key, java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter)
```
- Get rows from a table based on provided filter list
```java
public <T extends KVPersistable> java.util.List<T> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.FilterList filterlist)
```
- Get rows from a table based on provided filter list with starting rowkey (inclusive) and ending rowkey (exclusive)
```java
public <T extends KVPersistable> java.util.List<T> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.FilterList filterlist, java.lang.String startRow, java.lang.String endRow)
```
- Get row from a table for given rowkey and provided filter list
```java
public <T extends KVPersistable> T query(java.lang.String row_key, java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.FilterList filterlist)
```
- Get a selected list of columns (row key included implicitly) based on provided filter
```java
public <T extends KVPersistable> java.util.List<java.util.Map<java.lang.String,java.lang.Object>> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter, java.lang.String... columns)
```
- Get a selected list of columns (row key included implicitly) for all rows
```java
public <T extends KVPersistable> java.util.List<java.util.Map<java.lang.String,java.lang.Object>> query(java.lang.String table, java.lang.Class<T> entityClass, java.lang.String... columns)
```
- Get a selected list of columns (row key included implicitly) for given row key
```java
public <T extends KVPersistable> java.util.Map<java.lang.String,java.lang.Object> query(java.lang.String row_key, java.lang.String table, java.lang.Class<T> entityClass, java.lang.String... columns)
```
- Get a selected list of columns (row key included implicitly) for rows matching with provided filter
```java
public <T extends KVPersistable> java.util.List<java.util.Map<java.lang.String,java.lang.Object>> query(java.lang.String table, java.lang.Class<T> entityClass, org.apache.hadoop.hbase.filter.Filter filter, java.lang.String... columns)
```

--------------------
OTHER CONSIDERATIONS
--------------------
- HBASE Connection is thread safe and is a very heavy object. It's recommended to use a single connection through out the application.

# mitti
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
  
  - More examples on using the CRUD operations can be seen _HbaseDriverTest_
  
----------------
DRIVER FEATURES
----------------
Hbase driver supports following operations:
- addUpdate a single entity
```java
public <T extends KVPersistable> boolean addUpdate(T t,
                                                   java.lang.String queryTable,
                                                   java.lang.Class<T> entityClass)
```
  

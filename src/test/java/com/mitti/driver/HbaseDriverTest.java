package com.mitti.driver;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import com.mitti.models.SampleEntity;

@SuppressWarnings("deprecation")
public class HbaseDriverTest {

	private static final String ENTITY_TABLE = "tbl_entity";
	private static SampleEntity testEntity1 = null;
	private static SampleEntity testEntity2 = null;
	private static SampleEntity testEntity3 = null;
	private static SampleEntity testEntity4 = null;
	private static SampleEntity testEntity5 = null;

	private HbaseDriver hBaseDriver;

	private boolean isEntityAlreadyCreated = false;

	private void prepareHbaseDriver() throws IOException {

		Properties properties = new Properties();
		properties.load(HbaseDriverTest.class.getResourceAsStream("/application.properties"));

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
		config.set("hbase.zookeeper.property.clientPort",
				properties.getProperty("hbase.zookeeper.property.clientPort"));

		Connection connection = ConnectionFactory.createConnection(config);
		this.hBaseDriver = new HbaseDriver(connection, properties);
	}

	@Before
	public void shouldCreateTestEntities_SetUp() throws Exception {

		if (!isEntityAlreadyCreated) {

			prepareHbaseDriver();

			// TESTING: ENTITY DELETION
			List<SampleEntity> testEntities = hBaseDriver.query(ENTITY_TABLE, SampleEntity.class);

			for (SampleEntity testEntity : testEntities) {
				boolean deleteResult = hBaseDriver.deleteById(testEntity.get_Row_key(), ENTITY_TABLE);
				assertThat(deleteResult, is(true));
			}

			// TESTING: ENTITY CREATION
			testEntity1 = new SampleEntity();
			testEntity1.setRow_key("1");
			testEntity1.setBasic_age(18);
			testEntity1.setBasic_name("Kishore Kumar");
			testEntity1.setOther_entity_score(21.33F);
			Map<String, String> groupedMap1 = new HashMap<String, String>();
			groupedMap1.put("23", "Calcutta");
			groupedMap1.put("14", "Dilli");
			testEntity1.setVarcf(groupedMap1);

			testEntity2 = new SampleEntity();
			testEntity2.setRow_key("2");
			testEntity2.setBasic_age(25);
			testEntity2.setBasic_name("Arijit Singh");
			testEntity2.setOther_entity_score(51.728F);
			Map<String, String> groupedMap2 = new HashMap<String, String>();
			groupedMap2.put("23", "Kolkata");
			groupedMap2.put("14", "Delhi");
			testEntity2.setVarcf(groupedMap2);

			/*
			 * TESTING ENTITY CREATION
			 */
			try {
				boolean addResult1 = hBaseDriver.addUpdate(testEntity1, ENTITY_TABLE, SampleEntity.class);
				boolean addResult2 = hBaseDriver.addUpdate(testEntity2, ENTITY_TABLE, SampleEntity.class);
				assertThat(addResult1, is(true));
				assertThat(addResult2, is(true));
			} catch (Exception e) {
				e.printStackTrace();
				fail("Should not have thrown any exception while creating entities");
			}

			/*
			 * TESTING ENTITY SAVE ALL IN COLLECTION
			 */
			testEntity3 = new SampleEntity();
			testEntity3.setRow_key("3");
			testEntity3.setBasic_age(28);
			testEntity3.setBasic_name("Sonu Nigam");
			testEntity3.setOther_entity_score(31.7F);
			Map<String, String> groupedMap3 = new HashMap<String, String>();
			groupedMap3.put("43", "Calcutta");
			groupedMap3.put("54", "Dilli");
			testEntity3.setVarcf(groupedMap3);

			testEntity4 = new SampleEntity();
			testEntity4.setRow_key("4");
			testEntity4.setBasic_age(55);
			testEntity4.setBasic_name("Mohammed Rafi");
			testEntity4.setOther_entity_score(617.48F);
			Map<String, String> groupedMap4 = new HashMap<String, String>();
			groupedMap4.put("73", "Shimla");
			groupedMap4.put("94", "Manali");
			testEntity4.setVarcf(groupedMap4);

			testEntity5 = new SampleEntity();
			testEntity5.setRow_key("5");
			testEntity5.setBasic_age(55);
			testEntity5.setBasic_name("Mohammed Shami");
			testEntity5.setOther_entity_score(1007.9F);
			Map<String, String> groupedMap5 = new HashMap<String, String>();
			groupedMap5.put("73", "Bhatinda");
			groupedMap5.put("94", "Kenchi");
			testEntity5.setVarcf(groupedMap5);

			List<SampleEntity> SampleEntities = Arrays.asList(testEntity3, testEntity4, testEntity5);

			try {
				boolean addResultBulk = hBaseDriver.addUpdateAll(SampleEntities, ENTITY_TABLE, SampleEntity.class);
				assertThat(addResultBulk, is(true));
			} catch (Exception e) {
				e.printStackTrace();
				fail("Should not have thrown any exception while creating entities");
			}

			/*
			 * TESTING SELECTED COLUMNS DELETION BY ROW KEY
			 */

			try {
				hBaseDriver.deleteColumnsById("5", ENTITY_TABLE, "basic", "age");
			} catch (Exception e) {
				e.printStackTrace();
				fail("Should not have thrown any exception while deleting column of entities");
			}
			isEntityAlreadyCreated = true;
		}
	}

	/*
	 * TESTING ENTITY FETCH BY ID
	 */
	@Test
	public void shouldGetSampleTestEntitiesById() {

		SampleEntity actualEntity = hBaseDriver.query("1", ENTITY_TABLE, SampleEntity.class);
		assertThat(actualEntity, is(testEntity1));
	}

	/*
	 * TESTING ALL ENTITIES FETCH
	 */
	@Test
	public void shouldGetAllSampleTestEntities() {

		List<SampleEntity> actualEntities = hBaseDriver.query(ENTITY_TABLE, SampleEntity.class);
		testEntity5.setBasic_age(null); // This is because age column is deleted above
		assertThat(actualEntities, hasItems(testEntity1, testEntity2, testEntity3, testEntity4, testEntity5));
	}

	/*
	 * TESTING ALL ENTITIES FETCH By COLUMN-VALUE FILTER This would return fully
	 * populated objects with all columns from matching records
	 */
	@Test
	public void shouldGetAllSampleTestEntitiesWithMatchingColumnValue() {

		SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("basic"), Bytes.toBytes("name"),
				CompareOp.EQUAL, Bytes.toBytes("Kishore Kumar"));
		nameFilter.setLatestVersionOnly(true);
		nameFilter.setFilterIfMissing(true);

		List<SampleEntity> actualEntities = hBaseDriver.query(ENTITY_TABLE, SampleEntity.class, nameFilter);
		assertThat(actualEntities.size(), is(1));
		assertThat(actualEntities, hasItems(testEntity1));
	}

	/*
	 * TESTING ALL ENTITIES FETCH By COLUMN-NAME FILTER This would return partially
	 * populated objects with row_key and matching columns from matching records
	 */
	@Test
	public void shouldGetAllSampleTestEntitiesWithMatchingColumnName() {

		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));

		List<SampleEntity> actualEntities = hBaseDriver.query(ENTITY_TABLE, SampleEntity.class, columnPrefixFilter);
		assertThat(actualEntities.size(), is(5));

		SampleEntity fetchedTestEntity = actualEntities.get(0);

		// TEST: no other columns are being fetched
		assertNull(fetchedTestEntity.getBasic_age());
		assertNull(fetchedTestEntity.getOther_entity_score());
		assertNull(fetchedTestEntity.getVarcf());

		// TEST: only row_key and name columns are being fetched
		assertNotNull(fetchedTestEntity.get_Row_key());
		assertNotNull(fetchedTestEntity.getBasic_name());
	}

	/*
	 * TESTING SELECTED COLUMNS FETCH FROM A TABLE FOR ALL ROWS
	 */

	@Test
	public void shouldGetSelectedColumnsAsMap() {

		List<Map<String, Object>> actualEntities = hBaseDriver.query(ENTITY_TABLE, SampleEntity.class, "name", "age",
				"varcf");
		assertThat(actualEntities.size(), is(5));

		Map<String, Object> fetchedTestEntity = actualEntities.get(0);

		// TEST: no other columns are being fetched
		assertNull(fetchedTestEntity.get("entity_score"));

		// TEST: only row_key and name columns are being fetched
		assertNotNull(fetchedTestEntity.get("row_key"));
		assertNotNull(fetchedTestEntity.get("age"));
		assertNotNull(fetchedTestEntity.get("name"));
		assertNotNull(fetchedTestEntity.get("varcf"));
	}

	/*
	 * TESTING SELECTED COLUMNS FETCH FROM A TABLE FOR SINGLE ROW
	 */
	@Test
	public void shouldGetSelectedColumnsForId() {

		String cols = "name,age,entity_score";

		Map<String, Object> fetchedTestEntity = hBaseDriver.query("1", ENTITY_TABLE, SampleEntity.class,
				cols.split(","));

		// TEST: no other columns are being fetched
		assertNull(fetchedTestEntity.get("varcf_map"));

		// TEST: only row_key and name columns are being fetched
		assertNotNull(fetchedTestEntity.get("row_key"));
		assertNotNull(fetchedTestEntity.get("age"));
		assertNotNull(fetchedTestEntity.get("name"));
		assertNotNull(fetchedTestEntity.get("entity_score"));
	}

	/*
	 * TESTING ENTITY UPDATE
	 */
	@Test
	public void shouldUpdateSampleEntity() {

		float new_score = 9765.54F;
		testEntity1.setOther_entity_score(new_score);
		;
		// UPDATE
		boolean updateResult = hBaseDriver.addUpdate(testEntity1, ENTITY_TABLE, SampleEntity.class);
		assertThat(updateResult, is(true));

		// FETCH UPDATED ROW
		SampleEntity actualEntity = hBaseDriver.query("1", ENTITY_TABLE, SampleEntity.class);
		assertThat(actualEntity.getOther_entity_score(), is(new_score));
	}
}
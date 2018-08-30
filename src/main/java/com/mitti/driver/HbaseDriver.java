package com.mitti.driver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mitti.common.DynamicColumnFamily;
import com.mitti.models.KVPersistable;

/**
 * @author Manoj Kumar Vohra
 */
@SuppressWarnings("deprecation")
public class HbaseDriver {

	private static final String FAILED_TO_SET_FIELD_VALUE = "Failed to set field value against setter method:";
	private static final String ROW_KEY = "row_key";
	private static final String NO_MATCHING_RECORD_FOUND_BY_ID_IN_TABLE = "No matching record found by Id: %s in table: %s";
	private static final String EXCEPTION_OCCURED_WHILE_QUERYING_DATA = "Exception Occured While Querying Data: ";
	private static final String EXCEPTION_OCCURED_WHILE_DELETING_DATA = "Exception Occured While Deleting Data: ";
	private static final String ROW_KEY_NOT_DEFINED_FOR_ENTITY_CLASS = "Row Key not defined for entity class: ";
	private static final String EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA_IN_LIST = "Exception Occured While Inserting/Updating Data In list: ";
	private static final String EXCEPTION_OCCURED_WHILE_CLOSING_TABLE = "Exception Occured While Closing Table: ";
	private static final String EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA = "Exception Occured While Inserting/Updating Data: ";
	private static final String UNDERSCORE = "_";
	private static final String EXCEPTION_OCCURED_WHILE_BUILDING_OBJECT_FOR = "Exception Occured While Building Object: ";

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Connection connection = null;
	private String tablePrefix = "";

	private Properties environmentProperties;

	public HbaseDriver(Connection connection, Properties env) {
		this.connection = connection;
		this.environmentProperties = env;
		initMaprDbPrefixIfrequired();
	}

	private void initMaprDbPrefixIfrequired() {
		Boolean useMapRDb = Boolean.valueOf(environmentProperties.getProperty("hbase.usemaprdb"));
		if (useMapRDb) {
			this.tablePrefix = environmentProperties.getProperty("hbase.maprdb.path");
		}
	}

	public <T extends KVPersistable> boolean addUpdate(T t, String queryTable, Class<T> entityClass) {

		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */
		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> groupedFamilies = new LinkedList<String>();
		List<String> groupedFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				String family = field.split(UNDERSCORE, 2)[0];
				groupedFamilies.add(family.toLowerCase());
				groupedFields.add(field.toLowerCase());
			}
		}

		List<Field> fields = getApplicableFields(entityClass);
		Table table = null;
		boolean addUpdateDone = false;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Put p = prepareAndGetPut(t, entityClass, groupedFamilies, groupedFields, fields);
			table.put(p);
			addUpdateDone = true;
		} catch (NullPointerException e) {
			logger.error(EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA + " Table:" + queryTable + "\n"
					+ ExceptionUtils.getFullStackTrace(e) + "\nObject:\n" + t.toString());
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA + " Table:" + queryTable + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(EXCEPTION_OCCURED_WHILE_CLOSING_TABLE + " Table:" + queryTable + "\n"
						+ ExceptionUtils.getFullStackTrace(e));
			}
		}

		return addUpdateDone;
	}

	public <T extends KVPersistable> boolean addUpdateAll(List<T> arrT, String queryTable, Class<T> entityClass) {

		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */
		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> groupedFamilies = new LinkedList<String>();
		List<String> groupedFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				String family = field.split(UNDERSCORE, 2)[0];
				groupedFamilies.add(family.toLowerCase());
				groupedFields.add(field.toLowerCase());
			}
		}

		List<Field> fields = getApplicableFields(entityClass);

		Table table = null;
		boolean addUpdateDone = false;
		T currentT = null;
		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			List<Put> allPuts = new ArrayList<Put>();
			for (T t : arrT) {
				currentT = t;
				Put p = prepareAndGetPut(t, entityClass, groupedFamilies, groupedFields, fields);
				allPuts.add(p);
			}
			table.put(allPuts);
			addUpdateDone = true;
		} catch (NullPointerException e) {
			logger.error(EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA_IN_LIST + " Table:" + queryTable + "\n"
					+ ExceptionUtils.getFullStackTrace(e) + "\nObject:\n" + currentT.toString());
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_INSERTING_UPDATING_DATA_IN_LIST + " Table:" + queryTable + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(EXCEPTION_OCCURED_WHILE_CLOSING_TABLE + " Table:" + queryTable + "\n"
						+ ExceptionUtils.getFullStackTrace(e));
			}
		}

		return addUpdateDone;
	}

	private <T extends KVPersistable> List<Field> getApplicableFields(Class<T> entityClass) {
		Field[] allFields = entityClass.getDeclaredFields();
		List<Field> fields = Arrays.stream(allFields).filter(f -> !(f.getName().equals(ROW_KEY)))
				.collect(Collectors.toList());
		fields.forEach(f -> f.setAccessible(true));
		return fields;
	}

	@SuppressWarnings("unchecked")
	private <T extends KVPersistable> Put prepareAndGetPut(T t, Class<T> entityClass, List<String> groupedFamilies,
			List<String> groupedFields, List<Field> fields)
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		Put p = null;

		String row_key = t.getRow_key();
		if (row_key != null) {
			p = new Put(Bytes.toBytes(row_key.toString()));
		} else {
			throw new IllegalArgumentException(
					ROW_KEY_NOT_DEFINED_FOR_ENTITY_CLASS + entityClass.getCanonicalName() + " Row Key:" + row_key);
		}

		for (Field field : fields) {

			String fieldName = field.getName();
			Object value = field.get(t);

			if (value != null) {
				String[] familyAndColumn = fieldName.split(UNDERSCORE, 2);
				String family = familyAndColumn[0];

				if (groupedFamilies.contains(family) && groupedFields.contains(fieldName)) {
					Map<String, String> columnValuesMap = (Map<String, String>) value;
					for (String columnName : columnValuesMap.keySet()) {
						String columnValue = columnValuesMap.get(columnName);
						p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
					}
				} else {
					String column = familyAndColumn[1];
					p.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value.toString()));
				}
			}

		}
		return p;
	}

	public <T extends KVPersistable> boolean deleteByFilter(String queryTable, Class<T> entityClass, Filter filter) {

		Table table = null;
		List<T> entities = query(queryTable, entityClass, filter);
		boolean isDeleted = true;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));

			for (T t : entities) {
				String row_key = null;
				try {
					row_key = t.getRow_key();
					Delete delete = new Delete(Bytes.toBytes(row_key));
					table.delete(delete);
				} catch (Exception e) {
					isDeleted = false;
					logger.error(EXCEPTION_OCCURED_WHILE_DELETING_DATA + "Row Key:" + row_key + "\n"
							+ ExceptionUtils.getFullStackTrace(e));
				}
			}
		} catch (Exception e) {
			isDeleted = false;
			logger.error(EXCEPTION_OCCURED_WHILE_DELETING_DATA + "\n" + ExceptionUtils.getFullStackTrace(e));
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(EXCEPTION_OCCURED_WHILE_CLOSING_TABLE + " Table:" + queryTable + "\n"
						+ ExceptionUtils.getFullStackTrace(e));
			}
		}

		return isDeleted;
	}

	public boolean deleteById(String row_key, String queryTable) {

		Table table = null;

		boolean isDeleted = false;
		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Delete delete = new Delete(Bytes.toBytes(row_key));
			table.delete(delete);
			isDeleted = true;
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_DELETING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(EXCEPTION_OCCURED_WHILE_CLOSING_TABLE + " Table:" + queryTable + "\n"
						+ ExceptionUtils.getFullStackTrace(e));
			}
		}

		return isDeleted;
	}

	public boolean deleteColumnsById(String row_key, String queryTable, String columnFamily, String... columns) {

		Table table = null;

		boolean isDeleted = false;
		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Delete delete = new Delete(Bytes.toBytes(row_key));
			for (String column : columns) {
				delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
			}
			table.delete(delete);
			isDeleted = true;
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_DELETING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		} finally {
			try {
				table.close();
			} catch (IOException e) {
				logger.error(EXCEPTION_OCCURED_WHILE_CLOSING_TABLE + " Table:" + queryTable + "\n"
						+ ExceptionUtils.getFullStackTrace(e));
			}
		}

		return isDeleted;
	}

	public <T extends KVPersistable> List<Map<String, Object>> query(String table, Class<T> entityClass, Filter filter,
			String... columns) {

		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */
		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> groupedFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				groupedFields.add(field.toLowerCase());
			}
		}

		FilterList topLevelFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		topLevelFilter.addFilter(filter);

		List<Map<String, Object>> queryResults = Collections.<Map<String, Object>>emptyList();

		try {

			FilterList selectedColFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

			for (String col : columns) {

				if (groupedFields.contains(col)) {
					String family = col.split(UNDERSCORE, 2)[0];
					FamilyFilter familyFilter = new FamilyFilter(CompareOp.EQUAL,
							new BinaryComparator(Bytes.toBytes(family)));
					selectedColFilterList.addFilter(familyFilter);
				} else {
					ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(col));
					selectedColFilterList.addFilter(columnPrefixFilter);
				}
			}

			topLevelFilter.addFilter(selectedColFilterList);

			queryResults = queryColumnOrientedResultForAll(table, entityClass, topLevelFilter, columns);

		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<Map<String, Object>> query(String table, Class<T> entityClass,
			String... columns) {

		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */
		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> groupedFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				groupedFields.add(field.toLowerCase());
			}
		}

		List<Map<String, Object>> queryResults = Collections.<Map<String, Object>>emptyList();

		try {

			FilterList selectedColFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

			for (String col : columns) {

				if (groupedFields.contains(col)) {
					String family = col.split(UNDERSCORE, 2)[0];
					FamilyFilter familyFilter = new FamilyFilter(CompareOp.EQUAL,
							new BinaryComparator(Bytes.toBytes(family)));
					selectedColFilterList.addFilter(familyFilter);
				} else {
					ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(col));
					selectedColFilterList.addFilter(columnPrefixFilter);
				}
			}

			queryResults = queryColumnOrientedResultForAll(table, entityClass, selectedColFilterList, columns);

		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> Map<String, Object> query(String row_key, String table, Class<T> entityClass,
			String... columns) {

		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */
		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> groupedFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				groupedFields.add(field.toLowerCase());
			}
		}

		Map<String, Object> queryResults = null;

		try {

			FilterList selectedColFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

			for (String col : columns) {

				if (groupedFields.contains(col)) {
					String family = col.split(UNDERSCORE, 2)[0];
					FamilyFilter familyFilter = new FamilyFilter(CompareOp.EQUAL,
							new BinaryComparator(Bytes.toBytes(family)));
					selectedColFilterList.addFilter(familyFilter);
				} else {
					ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes(col));
					selectedColFilterList.addFilter(columnPrefixFilter);
				}
			}

			queryResults = queryColumnOrientedResultForId(row_key, table, entityClass, selectedColFilterList, columns);

		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<T> query(String table, Class<T> entityClass) {

		List<T> queryResults = Collections.<T>emptyList();

		try {
			queryResults = queryForAll(table, entityClass);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> T query(String row_key, String table, Class<T> entityClass) {

		T queryResults = null;

		try {
			queryResults = queryForId(row_key, table, entityClass);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<T> query(String table, Class<T> entityClass, Filter filter) {

		List<T> queryResults = Collections.<T>emptyList();

		try {
			queryResults = queryForAll(table, entityClass, filter);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<T> query(String table, Class<T> entityClass, Filter filter, String startRow) {

		List<T> queryResults = Collections.<T>emptyList();

		try {
			queryResults = queryForAll(table, entityClass, filter, startRow);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> T query(String row_key, String table, Class<T> entityClass, Filter filter) {

		T queryResults = null;

		try {
			queryResults = queryForId(row_key, table, entityClass, filter);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<T> query(String table, Class<T> entityClass, FilterList filterlist) {

		List<T> queryResults = Collections.<T>emptyList();

		try {
			queryResults = queryForAll(table, entityClass, filterlist);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> List<T> query(String table, Class<T> entityClass, FilterList filterlist,
			String startRow, String endRow) {

		List<T> queryResults = Collections.<T>emptyList();

		try {
			queryResults = queryForAll(table, entityClass, filterlist, startRow, endRow);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Table:" + table + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	public <T extends KVPersistable> T query(String row_key, String table, Class<T> entityClass,
			FilterList filterlist) {

		T queryResults = null;

		try {
			queryResults = queryForId(row_key, table, entityClass, filterlist);
		} catch (Exception e) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(e));
		}

		return queryResults;
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass) throws IOException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Scan scan = new Scan();
		scan.setCaching(20);
		return queryForAll(queryTable, entityClass, scan);
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass, Filter filter)
			throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Scan scan = new Scan();
		scan.setCaching(20);
		scan.setFilter(filter);
		return queryForAll(queryTable, entityClass, scan);
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass, Filter filter,
			String startRow) throws IOException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setCaching(20);
		scan.setFilter(filter);
		return queryForAll(queryTable, entityClass, scan);
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass,
			FilterList filterlist) throws IOException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Scan scan = new Scan();
		scan.setCaching(20);
		scan.setFilter(filterlist);
		return queryForAll(queryTable, entityClass, scan);
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass,
			FilterList filterlist, String startRow, String endRow) throws IOException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(endRow));
		scan.setCaching(20);
		scan.setFilter(filterlist);
		return queryForAll(queryTable, entityClass, scan);
	}

	private <T extends KVPersistable> List<Map<String, Object>> queryColumnOrientedResultForAll(String queryTable,
			Class<T> entityClass, FilterList filterlist, String... columns) throws IOException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Scan scan = new Scan();
		scan.setCaching(20);
		scan.setFilter(filterlist);
		return queryColumnOrientedResultsForAll(queryTable, entityClass, scan, columns);
	}

	private <T extends KVPersistable> T queryForId(String row_key, String queryTable, Class<T> entityClass)
			throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Get getForId = new Get(Bytes.toBytes(row_key));
		return queryForId(row_key, queryTable, entityClass, getForId);
	}

	private <T extends KVPersistable> T queryForId(String row_key, String queryTable, Class<T> entityClass,
			Filter filter) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		Get getForId = new Get(Bytes.toBytes(row_key));
		getForId.setFilter(filter);
		return queryForId(row_key, queryTable, entityClass, getForId);
	}

	private <T extends KVPersistable> T queryForId(String row_key, String queryTable, Class<T> entityClass,
			FilterList filterlist) throws IOException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		Get getForId = new Get(Bytes.toBytes(row_key));
		getForId.setFilter(filterlist);
		return queryForId(row_key, queryTable, entityClass, getForId);
	}

	private <T extends KVPersistable> Map<String, Object> queryColumnOrientedResultForId(String row_key,
			String queryTable, Class<T> entityClass, FilterList filterlist, String... columns) throws IOException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Get getForId = new Get(Bytes.toBytes(row_key));
		getForId.setFilter(filterlist);
		return queryColumnOrientedResultsForId(row_key, queryTable, entityClass, getForId, columns);
	}

	private <T extends KVPersistable> List<T> queryForAll(String queryTable, Class<T> entityClass, Scan scan)
			throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Pair<List<String>, List<String>> groupedFamilyFieldsTuple = getGroupedColumnFamiliesAndFields(entityClass);
		List<String> groupedFamilies = groupedFamilyFieldsTuple.getValue0();
		List<String> groupedFields = groupedFamilyFieldsTuple.getValue1();
		Map<String, Field> fieldsMap = getFieldsMap(entityClass);

		List<T> queryResults = new ArrayList<T>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();

			/*
			 * Iterate over each row and build the associated object
			 */
			while (iterator.hasNext()) {

				Result result = iterator.next();
				try {
					prepareResults(entityClass, groupedFamilies, groupedFields, fieldsMap, queryResults, table, result);
				} catch (Exception e) {
					logger.error(EXCEPTION_OCCURED_WHILE_BUILDING_OBJECT_FOR + entityClass + "\n"
							+ ExceptionUtils.getFullStackTrace(e));
				}
			}
		} catch (Exception x) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + ExceptionUtils.getFullStackTrace(x));
		} finally {
			table.close();
		}
		return queryResults;
	}

	private <T extends KVPersistable> List<Map<String, Object>> queryColumnOrientedResultsForAll(String queryTable,
			Class<T> entityClass, Scan scan, String... columns) throws IOException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		Pair<List<String>, List<String>> groupedFamilyFieldsTuple = getGroupedColumnFamiliesAndFields(entityClass);
		List<String> groupedFamilies = groupedFamilyFieldsTuple.getValue0();
		List<String> groupedFields = groupedFamilyFieldsTuple.getValue1();
		Set<String> fieldNamesSet = getFieldNames(entityClass);

		List<Map<String, Object>> queryResults = new ArrayList<Map<String, Object>>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			ResultScanner scanner = table.getScanner(scan);
			Iterator<Result> iterator = scanner.iterator();

			/*
			 * Iterate over each row and build the associated object
			 */
			while (iterator.hasNext()) {

				Result result = iterator.next();
				try {
					prepareColumnOrientedResults(entityClass, groupedFamilies, groupedFields, fieldNamesSet,
							queryResults, table, result, columns);
				} catch (Exception e) {
					logger.error(EXCEPTION_OCCURED_WHILE_BUILDING_OBJECT_FOR + entityClass + "\n"
							+ ExceptionUtils.getFullStackTrace(e));
				}
			}
		} catch (Exception x) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + ExceptionUtils.getFullStackTrace(x));
		} finally {
			table.close();
		}
		return queryResults;
	}

	private <T extends KVPersistable> Pair<List<String>, List<String>> getGroupedColumnFamiliesAndFields(
			Class<T> entityClass) {
		/*
		 * Checking if any dynamic cf grouping exists in the entity
		 * 
		 */

		DynamicColumnFamily dynamicColumnFamilyFields = entityClass.getAnnotation(DynamicColumnFamily.class);
		List<String> grpdFamilies = new LinkedList<String>();
		List<String> grpdFields = new LinkedList<String>();
		if (dynamicColumnFamilyFields != null) {
			String[] groupedFieldsArr = dynamicColumnFamilyFields.fields();
			for (String field : groupedFieldsArr) {
				String family = field.split(UNDERSCORE, 2)[0];
				grpdFamilies.add(family.toLowerCase());
				grpdFields.add(field.toLowerCase());
			}
		}
		Pair<List<String>, List<String>> groupedFamilyFieldsTuple = new Pair<List<String>, List<String>>(grpdFamilies,
				grpdFields);
		return groupedFamilyFieldsTuple;
	}

	private <T extends KVPersistable> Set<String> getFieldNames(Class<T> entityClass) {
		Field[] fields = entityClass.getDeclaredFields();
		Set<String> fieldNamesSet = Arrays.stream(fields).map(m -> m.getName()).collect(Collectors.toSet());
		return fieldNamesSet;
	}

	private <T extends KVPersistable> Map<String, Field> getFieldsMap(Class<T> entityClass) {

		Map<String, Field> fieldsMap = new HashMap<String, Field>();

		Field[] fields = entityClass.getDeclaredFields();

		for (Field field : fields) {
			field.setAccessible(true);
			fieldsMap.put(field.getName(), field);
		}

		return fieldsMap;
	}

	private <T extends KVPersistable> T queryForId(String row_key, String queryTable, Class<T> entityClass,
			Get getForId) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		Pair<List<String>, List<String>> groupedFamilyFieldsTuple = getGroupedColumnFamiliesAndFields(entityClass);
		List<String> groupedFamilies = groupedFamilyFieldsTuple.getValue0();
		List<String> groupedFields = groupedFamilyFieldsTuple.getValue1();

		Map<String, Field> fieldsMap = getFieldsMap(entityClass);

		List<T> queryResults = new ArrayList<T>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Result result = table.get(getForId);

			if (result == null || result.getMap() == null) {
				logger.info(String.format(NO_MATCHING_RECORD_FOUND_BY_ID_IN_TABLE, row_key, queryTable));
				return null;
			}
			prepareResults(entityClass, groupedFamilies, groupedFields, fieldsMap, queryResults, table, result);
		} catch (Exception x) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + "Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(x));
		} finally {
			table.close();
		}
		return queryResults.size() > 0 ? queryResults.get(0) : null;
	}

	private <T extends KVPersistable> Map<String, Object> queryColumnOrientedResultsForId(String row_key,
			String queryTable, Class<T> entityClass, Get getForId, String... columns) throws IOException,
			InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		Pair<List<String>, List<String>> groupedFamilyFieldsTuple = getGroupedColumnFamiliesAndFields(entityClass);
		List<String> groupedFamilies = groupedFamilyFieldsTuple.getValue0();
		List<String> groupedFields = groupedFamilyFieldsTuple.getValue1();
		Set<String> fieldNamesSet = getFieldNames(entityClass);

		List<Map<String, Object>> queryResults = new ArrayList<Map<String, Object>>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Result result = table.get(getForId);

			if (result == null || result.getMap() == null) {
				logger.info(String.format(NO_MATCHING_RECORD_FOUND_BY_ID_IN_TABLE, row_key, queryTable));
				return null;
			}

			prepareColumnOrientedResults(entityClass, groupedFamilies, groupedFields, fieldNamesSet, queryResults,
					table, result, columns);
		} catch (Exception x) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(x));
		} finally {
			table.close();
		}
		return queryResults.size() > 0 ? queryResults.get(0) : null;
	}

	private <T extends KVPersistable> void prepareColumnOrientedResults(Class<T> entityClass,
			List<String> groupedFamilies, List<String> groupedFields, Set<String> fieldNames,
			List<Map<String, Object>> queryResults, Table table, Result result, String... columns)
			throws IOException, InstantiationException, IllegalAccessException {

		Map<String, Object> columnValuesMap = new HashMap<String, Object>();

		String rowkey = Bytes.toString(result.getRow());
		columnValuesMap.put(ROW_KEY, rowkey);

		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : result.getMap()
				.entrySet()) {

			String columnFamily = Bytes.toString(columnFamilyMap.getKey()).toLowerCase();

			if (groupedFamilies.contains(columnFamily)) {

				String column_name = groupedFields.get(groupedFamilies.indexOf(columnFamily));
				Map<String, String> groupedColumnValuesMap = new HashMap<String, String>();

				for (Entry<byte[], NavigableMap<Long, byte[]>> versionEntry : columnFamilyMap.getValue().entrySet()) {

					Entry<Long, byte[]> entry = versionEntry.getValue().lastEntry();
					String column = Bytes.toString(versionEntry.getKey());
					String value = Bytes.toString(entry.getValue());

					// Special case: if column family contains fixed fields
					// apart from dynamic fields
					List<String> askedColsList = Arrays.asList(columns);
					if (fieldNames.contains(columnFamily + UNDERSCORE + column)) {
						if (askedColsList.contains(column)) {
							columnValuesMap.put(column, value);
						}

					} else {
						groupedColumnValuesMap.put(column, value);
					}

				}

				if (!groupedColumnValuesMap.isEmpty()) {
					columnValuesMap.put(column_name, groupedColumnValuesMap);
				}

			} else {

				for (Entry<byte[], NavigableMap<Long, byte[]>> versionEntry : columnFamilyMap.getValue().entrySet()) {

					Entry<Long, byte[]> entry = versionEntry.getValue().lastEntry();
					String column = Bytes.toString(versionEntry.getKey());
					String value = Bytes.toString(entry.getValue());
					columnValuesMap.put(column, value);
				}
			}

		}
		queryResults.add(columnValuesMap);
	}

	private <T extends KVPersistable> void prepareResults(Class<T> entityClass, List<String> groupedFamilies,
			List<String> groupedFields, Map<String, Field> fieldsMap, List<T> queryResults, Table table, Result result)
			throws IOException, InstantiationException, IllegalAccessException {

		Map<String, Object> columnValuesMap = new HashMap<String, Object>();

		String rowkey = Bytes.toString(result.getRow());
		columnValuesMap.put(ROW_KEY, rowkey);
		Set<String> fieldNameSet = fieldsMap.keySet();

		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyMap : result.getMap()
				.entrySet()) {

			String columnFamily = Bytes.toString(columnFamilyMap.getKey()).toLowerCase();

			if (groupedFamilies.contains(columnFamily)) {

				String column_name = groupedFields.get(groupedFamilies.indexOf(columnFamily));
				Map<String, String> groupedColumnValuesMap = new HashMap<String, String>();

				for (Entry<byte[], NavigableMap<Long, byte[]>> versionEntry : columnFamilyMap.getValue().entrySet()) {

					Entry<Long, byte[]> entry = versionEntry.getValue().lastEntry();
					String column = Bytes.toString(versionEntry.getKey());
					String value = Bytes.toString(entry.getValue());

					// Special case: if column family contains fixed fields
					// apart from dynamic fields
					if (fieldNameSet.contains(columnFamily + UNDERSCORE + column)) {
						columnValuesMap.put(columnFamily + UNDERSCORE + column, value);
					} else {
						groupedColumnValuesMap.put(column, value);
					}
				}

				if (!groupedColumnValuesMap.isEmpty()) {
					columnValuesMap.put(column_name, groupedColumnValuesMap);
				}

			} else {

				for (Entry<byte[], NavigableMap<Long, byte[]>> versionEntry : columnFamilyMap.getValue().entrySet()) {

					Entry<Long, byte[]> entry = versionEntry.getValue().lastEntry();
					String column = columnFamily + UNDERSCORE + Bytes.toString(versionEntry.getKey());
					String value = Bytes.toString(entry.getValue());
					columnValuesMap.put(column, value);
				}
			}

		}

		T t = (T) entityClass.newInstance();

		for (String column : columnValuesMap.keySet()) {
			Object valueToBeSet = columnValuesMap.get(column);
			Field field = fieldsMap.get(column);

			if (field != null) {

				Object convertedValue = null;
				if (valueToBeSet instanceof HashMap) {
					convertedValue = valueToBeSet;
				} else {
					convertedValue = getValueForType((String) valueToBeSet, field.getType().getSimpleName());
				}

				try {
					field.set(t, convertedValue);
				} catch (Exception x) {
					logger.error(FAILED_TO_SET_FIELD_VALUE + column + " Value:" + valueToBeSet + " Row Key:" + rowkey
							+ " Entity:" + entityClass.getName());
				}
			}

		}

		queryResults.add(t);

	}

	private Object getValueForType(String valueToBeSet, String type) {

		switch (type) {

		case "String":
			return valueToBeSet;

		case "short":
		case "Short":
			return Short.valueOf(valueToBeSet);

		case "long":
		case "Long":
			return Long.valueOf(valueToBeSet);

		case "int":
		case "Integer":
			return Integer.valueOf(valueToBeSet);

		case "double":
		case "Double":
			return Double.valueOf(valueToBeSet);

		case "float":
		case "Float":
			return Float.valueOf(valueToBeSet);

		case "boolean":
		case "Boolean":
			return Boolean.valueOf(valueToBeSet);

		default:
			throw new IllegalArgumentException("Unsupported Field Type: " + type + " Value: [" + valueToBeSet + "]");
		}

	}
}
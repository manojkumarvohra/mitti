package com.mitti.driver;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

import org.apache.commons.lang.StringUtils;
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
	private static final String GETTER_METHOD_PREFIX = "get";
	private static final String SETTER_METHOD_PREFIX = "set";
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

		Table table = null;
		boolean addUpdateDone = false;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Put p = prepareAndGetPut(t, entityClass);
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

		Table table = null;

		boolean addUpdateDone = false;
		T currentT = null;
		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			List<Put> allPuts = new ArrayList<Put>();
			for (T t : arrT) {
				currentT = t;
				Put p = prepareAndGetPut(t, entityClass);
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

	@SuppressWarnings("unchecked")
	private <T extends KVPersistable> Put prepareAndGetPut(T t, Class<T> entityClass)
			throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

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

		Put p = null;

		String row_key = t.get_Row_key();
		if (row_key != null) {
			p = new Put(Bytes.toBytes(row_key.toString()));
		} else {
			throw new IllegalArgumentException(
					ROW_KEY_NOT_DEFINED_FOR_ENTITY_CLASS + entityClass.getCanonicalName() + " Row Key:" + row_key);
		}

		Method[] allMethods = entityClass.getDeclaredMethods();

		for (Method method : allMethods) {

			String methodName = method.getName();

			if (methodName.startsWith(GETTER_METHOD_PREFIX)
					&& !methodName.startsWith(GETTER_METHOD_PREFIX + UNDERSCORE)) {

				Object value = method.invoke(t);

				if (value != null) {
					String family_column_string = methodName.split(GETTER_METHOD_PREFIX, 2)[1].toLowerCase();
					String[] familyAndColumn = family_column_string.split(UNDERSCORE, 2);
					String family = familyAndColumn[0];
					String column = familyAndColumn[1];

					if (groupedFamilies.contains(family) && groupedFields.contains(family_column_string)) {

						Map<String, String> columnValuesMap = (Map<String, String>) value;
						for (String columnName : columnValuesMap.keySet()) {
							String columnValue = columnValuesMap.get(columnName);
							p.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(columnValue));
						}
					} else {
						p.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value.toString()));
					}
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
					row_key = t.get_Row_key();
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
					prepareResults(entityClass, queryResults, table, result);
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
					prepareColumnOrientedResults(entityClass, queryResults, table, result, columns);
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

	private <T extends KVPersistable> T queryForId(String row_key, String queryTable, Class<T> entityClass,
			Get getForId) throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {

		List<T> queryResults = new ArrayList<T>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Result result = table.get(getForId);

			if (result == null || result.getMap() == null) {
				logger.info(String.format(NO_MATCHING_RECORD_FOUND_BY_ID_IN_TABLE, row_key, queryTable));
				return null;
			}

			prepareResults(entityClass, queryResults, table, result);
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

		List<Map<String, Object>> queryResults = new ArrayList<Map<String, Object>>();
		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tablePrefix + queryTable));
			Result result = table.get(getForId);

			if (result == null || result.getMap() == null) {
				logger.info(String.format(NO_MATCHING_RECORD_FOUND_BY_ID_IN_TABLE, row_key, queryTable));
				return null;
			}

			prepareColumnOrientedResults(entityClass, queryResults, table, result, columns);
		} catch (Exception x) {
			logger.error(EXCEPTION_OCCURED_WHILE_QUERYING_DATA + " Row Key:" + row_key + "\n"
					+ ExceptionUtils.getFullStackTrace(x));
		} finally {
			table.close();
		}
		return queryResults.size() > 0 ? queryResults.get(0) : null;
	}

	private <T extends KVPersistable> void prepareColumnOrientedResults(Class<T> entityClass,
			List<Map<String, Object>> queryResults, Table table, Result result, String... columns)
			throws IOException, InstantiationException, IllegalAccessException {

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

		/*
		 * Build setter method map (once)
		 * 
		 */
		Map<String, Method> setterMethodsMap = new HashMap<String, Method>();
		Map<String, String> parameterMethodsMap = new HashMap<String, String>();
		Method[] methods = entityClass.getDeclaredMethods();

		for (Method method : methods) {

			String name = method.getName();

			if (name.startsWith(SETTER_METHOD_PREFIX)) {
				setterMethodsMap.put(name, method);
				String expectedParameterTypeName = method.getParameterTypes()[0].getSimpleName();
				parameterMethodsMap.put(name, expectedParameterTypeName);
			}
		}

		Set<String> setterMethodsMapKeySet = setterMethodsMap.keySet();

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
					if (setterMethodsMapKeySet.contains(
							SETTER_METHOD_PREFIX + StringUtils.capitalize(columnFamily) + UNDERSCORE + column)) {
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

	private <T extends KVPersistable> void prepareResults(Class<T> entityClass, List<T> queryResults, Table table,
			Result result) throws IOException, InstantiationException, IllegalAccessException {

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

		/*
		 * Build setter method map (once)
		 * 
		 */
		Map<String, Method> setterMethodsMap = new HashMap<String, Method>();
		Map<String, String> parameterMethodsMap = new HashMap<String, String>();
		Method[] methods = entityClass.getDeclaredMethods();

		for (Method method : methods) {

			String name = method.getName();

			if (name.startsWith(SETTER_METHOD_PREFIX)) {
				setterMethodsMap.put(name, method);
				String expectedParameterTypeName = method.getParameterTypes()[0].getSimpleName();
				parameterMethodsMap.put(name, expectedParameterTypeName);
			}
		}

		Set<String> setterMethodsMapKeySet = setterMethodsMap.keySet();

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
					if (setterMethodsMapKeySet.contains(
							SETTER_METHOD_PREFIX + StringUtils.capitalize(columnFamily) + UNDERSCORE + column)) {
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

			String setterMethodName = SETTER_METHOD_PREFIX + StringUtils.capitalize(column);
			Object valueToBeSet = columnValuesMap.get(column);
			Method setterMethod = setterMethodsMap.get(setterMethodName);

			if (setterMethod != null) {
				try {
					setterMethod.setAccessible(true);

					Object convertedValue = null;

					if (valueToBeSet instanceof HashMap) {
						convertedValue = valueToBeSet;
					} else {
						convertedValue = getValueForType((String) valueToBeSet,
								parameterMethodsMap.get(setterMethodName));
					}
					setterMethod.invoke(t, convertedValue);
				} catch (Exception x) {
					logger.error(FAILED_TO_SET_FIELD_VALUE + setterMethodName + " Value:" + valueToBeSet + " Row Key:"
							+ rowkey + " Entity:" + entityClass.getName());
				}
			}

		}

		queryResults.add(t);

	}

	private Object getValueForType(String valueToBeSet, String type) {

		switch (type) {

		case "String":
			return valueToBeSet;

		case "Short":
			return Short.valueOf(valueToBeSet);

		case "Long":
			return Long.valueOf(valueToBeSet);

		case "Integer":
			return Integer.valueOf(valueToBeSet);

		case "Double":
			return Double.valueOf(valueToBeSet);

		case "Float":
			return Float.valueOf(valueToBeSet);

		default:
			throw new IllegalArgumentException("Unsupported Field Type: " + type + " Value: [" + valueToBeSet + "]");
		}

	}
}
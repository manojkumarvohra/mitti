package com.mitti;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.map.HashedMap;

import com.mitti.driver.HbaseDriver;
import com.mitti.models.SampleEntity;

public class App {

	public static void main(String[] args) throws IOException {

		Properties properties = new Properties();
		properties.load(App.class.getResourceAsStream("/application.properties"));

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
		config.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"));

		Connection connection = ConnectionFactory.createConnection(config);
		
		HbaseDriver driver = new HbaseDriver(connection, properties);
		
		//Create - Update sample entity
		SampleEntity sampleEntity = new SampleEntity();
		sampleEntity.setRow_key("1");
		sampleEntity.setBasic_age(20);
		sampleEntity.setBasic_name("first name");
		sampleEntity.setOther_entity_score(21.99F);
		Map<String, String> variableFields = new HashedMap<String, String>();
		variableFields.put("op", "23");
		variableFields.put("bi", "yuw");
		sampleEntity.setVarcf(variableFields);
		driver.addUpdate(sampleEntity, "tbl_entity", SampleEntity.class);
		
		//For more examples check the HbaseDriverTest
	}
}
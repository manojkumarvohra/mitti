package com.mitti.models;

import java.util.Map;
import com.mitti.common.DynamicColumnFamily;

/**
 * @author Manoj Kumar Vohra
 * 
 */

// Annotate class with @DynamicColumnFamily if class contains fields which
// map variable set of columns under a column family. Provide name of the
// corresponding class fields in the annotation fields array.
@DynamicColumnFamily(fields = { "varcf_map" })
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
	private Map<String, String> varcf_map;

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

	public Map<String, String> getVarcf_map() {
		return varcf_map;
	}

	public void setVarcf_map(Map<String, String> varcf_map) {
		this.varcf_map = varcf_map;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((basic_age == null) ? 0 : basic_age.hashCode());
		result = prime * result + ((basic_name == null) ? 0 : basic_name.hashCode());
		result = prime * result + ((other_entity_score == null) ? 0 : other_entity_score.hashCode());
		result = prime * result + ((row_key == null) ? 0 : row_key.hashCode());
		result = prime * result + ((varcf_map == null) ? 0 : varcf_map.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SampleEntity other = (SampleEntity) obj;
		if (basic_age == null) {
			if (other.basic_age != null)
				return false;
		} else if (!basic_age.equals(other.basic_age))
			return false;
		if (basic_name == null) {
			if (other.basic_name != null)
				return false;
		} else if (!basic_name.equals(other.basic_name))
			return false;
		if (other_entity_score == null) {
			if (other.other_entity_score != null)
				return false;
		} else if (!other_entity_score.equals(other.other_entity_score))
			return false;
		if (row_key == null) {
			if (other.row_key != null)
				return false;
		} else if (!row_key.equals(other.row_key))
			return false;
		if (varcf_map == null) {
			if (other.varcf_map != null)
				return false;
		} else if (!varcf_map.equals(other.varcf_map))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SampleEntity [row_key=" + row_key + ", basic_name=" + basic_name + ", basic_age=" + basic_age
				+ ", other_entity_score=" + other_entity_score + ", varcf_map=" + varcf_map + "]";
	}
}
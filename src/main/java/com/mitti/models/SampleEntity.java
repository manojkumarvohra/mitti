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
	private int basic_age;

	/*
	 * ColumnFamily: other Column: entity_score
	 */
	private float other_entity_score;

	/*
	 * ColumnFamily: other Column: done_flag
	 */
	private Boolean other_done_flag;

	/*
	 * ColumnFamily: varcf Column: this can support variable number of columns by
	 * this declaration. This would help in cases for flexible schema.
	 */
	private Map<String, String> varcf;

	/*
	 * ColumnFamily: varcf Column: fixed_value
	 * 
	 * Dynamic column family may also contain fixed fields apart from variable
	 * fields
	 */
	private String varcf_fixed_value;

	@Override
	public String getRow_key() {
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

	public int getBasic_age() {
		return basic_age;
	}

	public void setBasic_age(int basic_age) {
		this.basic_age = basic_age;
	}

	public float getOther_entity_score() {
		return other_entity_score;
	}

	public void setOther_entity_score(float other_entity_score) {
		this.other_entity_score = other_entity_score;
	}

	public Map<String, String> getVarcf() {
		return varcf;
	}

	public void setVarcf(Map<String, String> varcf) {
		this.varcf = varcf;
	}

	public Boolean getOther_done_flag() {
		return other_done_flag;
	}

	public void setOther_done_flag(Boolean other_done_flag) {
		this.other_done_flag = other_done_flag;
	}

	public String getVarcf_fixed_value() {
		return varcf_fixed_value;
	}

	public void setVarcf_fixed_value(String varcf_fixed_value) {
		this.varcf_fixed_value = varcf_fixed_value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + basic_age;
		result = prime * result + ((basic_name == null) ? 0 : basic_name.hashCode());
		result = prime * result + ((other_done_flag == null) ? 0 : other_done_flag.hashCode());
		result = prime * result + Float.floatToIntBits(other_entity_score);
		result = prime * result + ((row_key == null) ? 0 : row_key.hashCode());
		result = prime * result + ((varcf == null) ? 0 : varcf.hashCode());
		result = prime * result + ((varcf_fixed_value == null) ? 0 : varcf_fixed_value.hashCode());
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
		if (basic_age != other.basic_age)
			return false;
		if (basic_name == null) {
			if (other.basic_name != null)
				return false;
		} else if (!basic_name.equals(other.basic_name))
			return false;
		if (other_done_flag == null) {
			if (other.other_done_flag != null)
				return false;
		} else if (!other_done_flag.equals(other.other_done_flag))
			return false;
		if (Float.floatToIntBits(other_entity_score) != Float.floatToIntBits(other.other_entity_score))
			return false;
		if (row_key == null) {
			if (other.row_key != null)
				return false;
		} else if (!row_key.equals(other.row_key))
			return false;
		if (varcf == null) {
			if (other.varcf != null)
				return false;
		} else if (!varcf.equals(other.varcf))
			return false;
		if (varcf_fixed_value == null) {
			if (other.varcf_fixed_value != null)
				return false;
		} else if (!varcf_fixed_value.equals(other.varcf_fixed_value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "SampleEntity [row_key=" + row_key + ", basic_name=" + basic_name + ", basic_age=" + basic_age
				+ ", other_entity_score=" + other_entity_score + ", other_done_flag=" + other_done_flag
				+ ", varcf_fixed_value=" + varcf_fixed_value + ", varcf=" + varcf + "]";
	}
}
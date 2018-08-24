package com.mitti.models;

/**
 * @author Manoj Kumar Vohra
 * 
 * Base interface to be  implemented by all entities to be persisted to HBase as Key Value association
 */
public interface KVPersistable {
	String get_Row_key();
	void setRow_key(String row_key);
}

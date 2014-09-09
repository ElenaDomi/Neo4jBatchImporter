package com.hp.neo4jimport.batchinsert;

import org.neo4j.graphdb.RelationshipType;

public class RelType implements RelationshipType {
	
	protected String name;

    public RelType update(Object value) {
        this.name = value.toString();
        return this;
    }

	@Override
	public String name() {
		return name;
	}

}

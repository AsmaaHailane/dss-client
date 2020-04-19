package io.nms.client.routing;

import io.vertx.core.json.JsonObject;

public class Node {
	final private String id;
	final private String name;

	public Node(String id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public Node(JsonObject jNode) {
		this.id = jNode.getString("_id");
		this.name = jNode.getString("name");
	}

	public String getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Node other = (Node) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}
	/*public String toString() {
       // return name;
    	return toJson().encodePrettily();
    }*/

	public JsonObject toJson() {
    	JsonObject json = new JsonObject();
    	json.put("id", id);
    	json.put("name", name);		
    	return json;
	}

}

package io.nms.client.routing;


import io.vertx.core.json.JsonObject;

public class Link {

	private final String id;
	private final Node source;
	private final Node target;
	// private final int weight;

	public Link(String id, Node source, Node target) {
		// this.weight = 0;
		this.id = id;
		this.source = source;
		this.target = target;
	}
	
	public Link(JsonObject jLink) {
		this.id = jLink.getString("_id");
		source = new Node(jLink.getJsonObject("source"));
		target = new Node(jLink.getJsonObject("target"));		
	}
	
	public String getId() {
		return id;
	}
	public Node getTarget() {
		return target;
	}

	public Node getSource() {
		return source;
	}

	/*
	 * public int getWeight() { return weight; }
	 */
	/*public String toString() {
    	return toJson().encodePrettily();
       // return  id + "," +source + "," + destination + "," + weight;
    }*/

	public JsonObject toJson() {
		JsonObject json = new JsonObject();
		json.put("id", id);
		json.put("src", source.getName());
		json.put("dst", target.getName());
		return json;
	}

}




package io.nms.client.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class Graph {

	private HashMap<String, Node> nodes = new HashMap<String, Node>();;
	private final List<Link> links = new ArrayList<Link>();	

	/*public Graph(List<Node> nodes, List<Link> links) {
		this.nodes = nodes; 
		this.links = links; 
	}*/
	
	public Graph() {
		this.nodes.clear();
		this.links.clear();
	}
	
	public Graph(JsonObject jGraph) {		
		jGraph.getJsonArray("nodes").forEach(node -> {
			Node n = new Node((JsonObject)node);
			this.nodes.put(n.getId(), n);
		});	
		jGraph.getJsonArray("links").forEach(link-> {
			String id = ((JsonObject)link).getString("_id");
			String sourceId = ((JsonObject)link).getString("source");
			String targetId = ((JsonObject)link).getString("target");
			Link l = new Link(id, nodes.get(sourceId), nodes.get(targetId));
			this.links.add(l);
		});
	}
	
	public boolean isSet() {
		return !(nodes.isEmpty() || links.isEmpty());
	}

	public HashMap<String, Node> getNodes() {
		return nodes;
	}

	public List<Link> getLinks() {
		return links;
	}

	public JsonObject getJsonGraph(){
		JsonArray jNodes = new JsonArray();
		JsonArray jLinks = new JsonArray();
		JsonObject jGraph = new JsonObject();
		nodes.values().forEach(node-> {
			jNodes.add(node.toJson());
		});		
		links.forEach(link-> {
			jLinks.add(link.toJson());
		});
		jGraph.put("nodes", jNodes);
		jGraph.put("links",jLinks);

		return jGraph;
	}

	/*public JsonObject toJson() {
		  return new JsonObject().put("content", getGraph());
		}*/



}

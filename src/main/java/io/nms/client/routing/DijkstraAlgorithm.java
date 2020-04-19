package io.nms.client.routing;



import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DijkstraAlgorithm {

	private HashMap<String, Node> nodes;
	private final List<Link> links;
	private Set<Node> settledNodes;
	private Set<Node> unSettledNodes;
	private Map<Node, Node> predecessors;
	private Map<Node, Integer> distance;

	public DijkstraAlgorithm(Graph graph) {
		// create a copy of the array so that we can operate on this array
		this.nodes = new HashMap<String, Node>(graph.getNodes());
		this.links = new ArrayList<Link>(graph.getLinks());		
	}
	
	/*public LinkedList<Node> getShortestPath(String sourceId, String targetId) {
		// if one of the nodes does not exist
		Node source = nodes.get(sourceId);  
		Node target = nodes.get(targetId);
		if ((source == null) || (target == null)) {
			return null;
		}
		execute(source);
		return getPath(target);
	}*/

	// from List<String> we can create JsonArray
	public List<String> getShortestPathById(String sourceId, String targetId) {
		// if one of the nodes does not exist
		Node source = nodes.get(sourceId);  
		Node target = nodes.get(targetId);
		if ((source == null) || (target == null)) {
			return null;
		}
		
		execute(source);
		Object[] nodeArray = getPath(target).toArray();
		
		List<String> pathIds = new ArrayList<String>();
	    for(int i = 0; i < nodeArray.length; i++) {
	         pathIds.add(((Node)nodeArray[i]).getId());
	    }
	    return pathIds;
	}

	private void execute(Node source) {
		settledNodes = new HashSet<Node>();
		unSettledNodes = new HashSet<Node>();
		distance = new HashMap<Node, Integer>();
		predecessors = new HashMap<Node, Node>();
		distance.put(source, 0);
		unSettledNodes.add(source);
		while (unSettledNodes.size() > 0) {
			Node node = getMinimum(unSettledNodes);
			settledNodes.add(node);
			unSettledNodes.remove(node);
			setPath(node);
		}
	}

	private void setPath(Node node) {
		List<Node> adjacentNodes = getNeighbors(node);
		for (Node target : adjacentNodes) {
			predecessors.put(target, node);
			unSettledNodes.add(target);
		}
	}


	private List<Node> getNeighbors(Node node) {
		List<Node> neighbors = new ArrayList<Node>();
		for (Link edge : links) {
			if (edge.getSource().equals(node)) {
				neighbors.add(edge.getTarget());
			}
			/*else if (edge.getTarget().equals(node)) {
				neighbors.add(edge.getSource())
			}*/
		}
		return neighbors;
	}

	private Node getMinimum(Set<Node> vertexes) {
		Node minimum = null;
		for (Node vertex : vertexes) {
			if (minimum == null) {
				minimum = vertex;
			} else {
				if (getShortestDistance(vertex) < getShortestDistance(minimum)) {
					minimum = vertex;
				}
			}
		}
		return minimum;
	}

	private int getShortestDistance(Node destination) {
		Integer d = distance.get(destination);
		if (d == null) {
			return Integer.MAX_VALUE;
		} else {
			return d;
		}
	}

	/*
	 * This method returns the path from the source to the selected target and
	 * NULL if no path exists
	 */
	private LinkedList<Node> getPath(Node target) {
		LinkedList<Node> path = new LinkedList<Node>();
		Node step = target;
		// check if a path exists
		if (predecessors.get(step) == null) {
			return null;
		}
		path.add(step);
		while (predecessors.get(step) != null) {
			step = predecessors.get(step);
			path.add(step);
		}
		// Put it into the correct order
		Collections.reverse(path);
		return path;
	}	   			 
}

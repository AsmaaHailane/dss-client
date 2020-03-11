package io.nms.client.common;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import io.nms.messages.Capability;
import io.nms.messages.Message;
import io.nms.messages.Receipt;
import io.nms.messages.Result;
import io.nms.messages.Specification;
import io.nms.storage.NmsEbMessage;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class TopologyServiceVerticle extends AmqpVerticle {
	private static final int TOPO_UPDATE_PERIOD_MS = 10000;
	private static final int RESET_PERIOD_S = 60;
	private static final int SPEC_PERIOD_MS = 5000;
	
	protected String serviceName = "nms.topology";
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected Instant lastUpdate = Instant.now();
	
	protected List<JsonObject> nodes = new ArrayList<JsonObject>();
	protected List<JsonObject> links = new ArrayList<JsonObject>();
	
	public void start(Future<Void> fut) {
		Future<Void> futBase = Future.future(promise -> super.start(promise));
		futBase.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				// update topo every 60s
				vertx.setPeriodic(TOPO_UPDATE_PERIOD_MS, id -> {
					LOG.info("Check for new topology capabilities");
					// reset known capabilities every 10mn
					if (lastUpdate.plusSeconds(RESET_PERIOD_S).isBefore(Instant.now())) {
						LOG.info("Reset discovered capabilities");
						resetTopology();
						knownCaps.clear();
						lastUpdate = Instant.now();
					}
					Future<List<Capability>> futCaps = Future
						.future(promise -> getTopologyCapabilities(promise));
					futCaps.setHandler(newCapsRes -> {
						if (newCapsRes.failed()) {
							fut.fail(newCapsRes.cause());
						} else {
							if (!newCapsRes.result().isEmpty()) {
								LOG.info("Use new topology capabilities");
								sendTopologySpecifications(newCapsRes.result());
							}
						}
					});
				});
				fut.complete();
			}
		});
	}
	
	@Override
	protected void setServiceApi() {
		eb.consumer(serviceName, message -> {
			NmsEbMessage nmsEbMsg = new NmsEbMessage(message);
			LOG.info("[" + serviceName + "] got query: " 
					+ nmsEbMsg.getAction() + " | "
					+ nmsEbMsg.getParams().encodePrettily());
			
			switch (nmsEbMsg.getAction())
			{
			case "get_service_info":
				getServiceInfo(nmsEbMsg);
				break;
			case "get_topology":
				getTopology(nmsEbMsg);
				break;
			default:
				replyUnknownAction(nmsEbMsg);
			}
		});
	}
	
	@Override
	protected void processResult(io.nms.messages.Message resultMsg) {
		// check if result expected
		if (activeSpecs.containsKey(resultMsg.getSchema())) {
			Result result = new Result(resultMsg);
			Future<Void> fut = Future.future(promise -> updateTopologyGraph(result, promise));
			fut.setHandler(res -> {
				if (res.succeeded()) {
					JsonObject currentTopology = new JsonObject()
							.put("nodes", nodes)
							.put("links", links);
					JsonObject ebPubMsg = new JsonObject()
						.put("service", serviceName)
						.put("content", currentTopology);
					LOG.info("publish topology: "+ebPubMsg.encodePrettily());
					eb.publish("nms.info.topology", ebPubMsg);
				}
			});
		}
	}
	
	/*---------- topology service processing ---------*/
	protected void getTopologyCapabilities(Future<List<Capability>> future) {
		List<Capability> newCaps = new ArrayList<Capability>();
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	for (Capability c : res.result()) {
	        		if (!c.getName().equals("topology")) {
	        			continue;
	        		} else if (!knownCaps.containsKey(c.getAgentId())) {
						knownCaps.put(c.getAgentId(), c);
						newCaps.add(c);
					}
					/*else if (!knownCaps.get(c.getAgentId()).getSchema().equals(c.getSchema())) {
						knownCaps.put(c.getAgentId(), c);
						newCaps.add(c);
					}*/
				}
	        	LOG.info(newCaps.size()+" new capabilities discovered.");
	        	LOG.info(knownCaps.size()+" already known capabilities.");
	        	future.complete(newCaps);
	        } else {
	        	LOG.error("Failed to update topoloy capabilities", res.cause());
	        	future.fail(res.cause());
	        }
		});
	}
	
	protected void sendTopologySpecifications(List<Capability> caps) {
		for (Capability c : caps) {	
			long stopTime = Instant.now().plusSeconds(RESET_PERIOD_S).toEpochMilli();
			c.setWhen("now ... "+String.valueOf(stopTime)+" / "+SPEC_PERIOD_MS);
			Specification spec = new Specification(c);
			
			LOG.info("Spec: "+Message.toJsonString(spec, true));
			
			Future<Receipt> rct = Future.future(promise -> sendSpecification(spec, promise));
			rct.setHandler(res -> {
				if (res.succeeded()) {
					LOG.info("Rct: "+Message.toJsonString(res.result(), true));
					if (!res.result().getErrors().isEmpty()) {
						LOG.error("Error in Receipt from "+spec.getAgentId());
					}
				} else {
					LOG.error("Failed to get Receipt from "+spec.getAgentId(), res.cause());
				}
			});
		}
	}
	
	private void updateTopologyGraph(Result res, Future<Void> future) {
		// get node name and type
		String[] nodeId = res.getAgentId().split("-");
		String nodeName = nodeId[0];
		String nodeType = "N/A";
		if (nodeId.length > 1) {
			nodeType = nodeId[1];
		}
		
		// add source node if does not exist
		// with status = active
		JsonObject snode = new JsonObject()
			.put("name", nodeName)
			.put("type", nodeType)
			.put("status", "ACTIVE");
		upsertNode(snode);
		
		int tg = res.getResults().indexOf("target");
		int st = res.getResults().indexOf("status");
		
		// add links
		for (List<String> r : res.getResultValues()) {
			String tname = r.get(tg);
			String lstatus = r.get(st);

			// add target node if does not exist
			// with status = active
			JsonObject tnode = new JsonObject()
				.put("name", tname)
				.put("type", "N/A")
				.put("status", "INACTIVE");
			upsertNode(tnode);
			
			JsonObject link = new JsonObject()
				.put("sname", nodeName)
				.put("tname", tname)
				.put("status", lstatus);
			upsertLink(link);
		}
		future.complete();
	}
	
	private void upsertLink(JsonObject link) {
		String newSname = link.getString("sname");
		String newTname = link.getString("tname");
		int linkFound = -1;
		int i = 0;
		for (JsonObject l : links) {
			String sname = l.getString("sname");
			String tname = l.getString("tname");
			if((sname.equals(newSname) && tname.equals(newTname))||(sname.equals(newTname)&&tname.equals(newSname))) {
				linkFound = i;
				break;
			}
			i++;
		}
		if (linkFound >= 0) {
			links.set(linkFound, link);
		} else {
			links.add(link);
		}
	}
	
	private void upsertNode(JsonObject node) {
		int nodeFound = -1;
		int i = 0;
		for (JsonObject n : nodes) {
			if(n.getString("name").equals(node.getString("name"))) {
				nodeFound = i;
				break;
			}
			i++;
		}
		if (nodeFound >= 0) {
			if (nodes.get(nodeFound).getString("status").equals("INACTIVE")) {
				nodes.set(nodeFound, node);
			}
		} else {
			nodes.add(node);
		}
	}
	
	private void resetTopology() {
		for (JsonObject n : nodes) {
			n.put("status", "INACTIVE");
		}
		for (JsonObject l : links) {
			l.put("status", "DOWN");
		}
	}
	/*------------------------------------------------*/
	
	/*--------------- API functions ----------------*/
	protected void replyUnknownAction(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("action", message.getAction());
		response.put("error", "unknown action");
		message.reply(response);	      
	}
	
	protected void getTopology(NmsEbMessage message) {
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_topology")
			.put("params", new JsonObject());

			eb.send("nms.storage", toStorageMsg, reply -> {
				if (reply.succeeded()) {
					JsonObject response = (JsonObject)reply.result().body();
					response.put("service", serviceName);
					response.put("action", message.getAction());
					message.reply(response);
				} else {
					JsonObject response = new JsonObject();
					response.put("service", serviceName);
					response.put("action", message.getAction());
					response.put("error", reply.cause());
					message.reply(response);
			    }
			});
	}
	
	/*----------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[Topology] Closing Service.");
		super.stop(stopFuture);
	}
}
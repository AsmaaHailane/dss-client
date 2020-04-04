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
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class TopologyServiceVerticle extends AmqpVerticle {
	private static final int TOPO_UPDATE_PERIOD_MS = 10000;
	private static final int RESET_PERIOD_S = 60;
	private static final int SPEC_PERIOD_MS = 5000;
	
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected Instant lastUpdate = Instant.now();
	
	//protected List<JsonObject> nodes = new ArrayList<JsonObject>();
	//protected List<JsonObject> links = new ArrayList<JsonObject>();
	
	public void start(Future<Void> fut) {
		serviceName = "nms.topology";
		Future<Void> futBase = Future.future(promise -> super.start(promise));
		futBase.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				// update topo every 60s
				/*vertx.setPeriodic(TOPO_UPDATE_PERIOD_MS, id -> {
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
				});*/
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
			case "add_node":
				addNode(nmsEbMsg);
				break;
			case "get_node":
				getNode(nmsEbMsg);
				break;
			case "add_link":
				addLink(nmsEbMsg);
				break;
			case "get_link":
				getLink(nmsEbMsg);
				break;
			case "get_all_nodes":
				getAllNodes(nmsEbMsg);
				break;
			case "get_all_links":
				getAllLinks(nmsEbMsg);
				break;
			case "del_node":
				deleteNode(nmsEbMsg);
				break;
			case "del_link":
				deleteLink(nmsEbMsg);
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
			/*Future<Void> fut = Future.future(promise -> updateTopologyGraph(result, promise));
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
			});*/
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
	
	/*------------------------------------------------*/
	
	/*--------------- API functions ----------------*/
	protected void replyUnknownAction(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("action", message.getAction());
		response.put("error", "unknown action");
		message.reply(response);	      
	}
	
	/* TODO: publish message info for relevant changes... */
	protected void addNode(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("name","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "node name not specified");
			message.reply(response);
			return;
		}
		if (params.getJsonArray("itfs", new JsonArray()).isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "node must have at least one interface");
			message.reply(response);
			return;
		}
		if (!params.containsKey("agent")) {
			params.put("agent", "");
		}
		params.put("status", "pending");
		
		JsonObject toStorageMsg = new JsonObject()
				.put("action", "add_node")
				.put("params", params);

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				message.reply(response);
				publishUpdatedTopology();
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});
	}
	protected void getNode(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "node Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_node")
			.put("params", params);

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
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});
	}
	protected void addLink(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("source","").isEmpty() || params.getString("target","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "link must have source and target node name");
			message.reply(response);
			return;
		}
		
		JsonArray nodes = new JsonArray()
				.add(params.getString("source"))
				.add(params.getString("target"));
		JsonObject checkNodesMsg = new JsonObject()
				.put("action", "get_nodes")
				.put("params", new JsonObject().put("nodes", nodes));

		eb.send("nms.storage", checkNodesMsg, reply1 -> {
			if (reply1.succeeded()) {
				JsonObject nodesResp = (JsonObject)reply1.result().body();
				if (nodesResp.containsKey("content")) {
					int d = nodesResp.getJsonObject("content").getJsonArray("docs").size();
					if (d == 2) {
						params.put("status", "pending");
						
						JsonObject addLinkMsg = new JsonObject()
								.put("action", "add_link")
								.put("params", params);

						eb.send("nms.storage", addLinkMsg, reply -> {
							if (reply.succeeded()) {
								JsonObject response = (JsonObject)reply.result().body();
								response.put("service", serviceName);
								response.put("action", message.getAction());
								message.reply(response);
								publishUpdatedTopology();
							} else {
								JsonObject response = new JsonObject();
								response.put("service", serviceName);
								response.put("action", message.getAction());
								response.put("error", reply.cause().getMessage());
								message.reply(response);
							}
						});		
					} else {
						JsonObject response = new JsonObject();
						response.put("service", serviceName);
						response.put("action", message.getAction());
						response.put("error", "specified nodes do not exist");
						message.reply(response);
					}
				} else {
					JsonObject response = new JsonObject();
					response.put("service", serviceName);
					response.put("action", message.getAction());
					response.put("error", "nodes checking failed");
				}
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply1.cause().getMessage());
				message.reply(response);
			}
		});
	}
	protected void getLink(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "link Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_link")
			.put("params", params);

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
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});		
	}
	protected void getAllNodes(NmsEbMessage message) {
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_all_nodes")
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
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});
	}
	protected void getAllLinks(NmsEbMessage message) {
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_all_links")
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
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});		
	}
	protected void deleteNode(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "node id not specified");
			message.reply(response);
			return;
		}
		
		// delete links related to the node
		Future<Void> delLinksFut = Future.future();
		JsonObject delLinksMsg = new JsonObject()
				.put("action", "del_links_by_node")
				.put("params", new JsonObject().put("_id", params.getString("_id")));
		eb.send("nms.storage", delLinksMsg, rep -> {
			if (rep.succeeded()) {
				JsonObject delLinksResp = (JsonObject)rep.result().body();
				if (delLinksResp.containsKey("content")) {
					delLinksFut.complete();
				} else {
					delLinksFut.fail(delLinksResp.getString("error"));
				}
			} else {
				delLinksFut.fail(rep.cause());
			}
		});

		delLinksFut.setHandler(ar -> {
			if (ar.failed()) {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", ar.cause().getMessage());
				message.reply(response);
			} else {
				// delete the node
				JsonObject delNodeMsg = new JsonObject()
						.put("action", "del_node")
						.put("params", params);

				eb.send("nms.storage", delNodeMsg, rep -> {
					if (rep.succeeded()) {
						JsonObject response = (JsonObject)rep.result().body();
						response.put("service", serviceName);
						response.put("action", message.getAction());
						message.reply(response);
						publishUpdatedTopology();
					} else {
						JsonObject response = new JsonObject();
						response.put("service", serviceName);
						response.put("action", message.getAction());
						response.put("error", rep.cause().getMessage());
						message.reply(response);
					}
				});
			}
		});
	}
	protected void deleteLink(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "link id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "del_link")
			.put("params", params);

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				message.reply(response);
				publishUpdatedTopology();
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply.cause().getMessage());
				message.reply(response);
				}
			});		
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
				response.put("error", reply.cause().getMessage());
				message.reply(response);
			}
		});
	}		
	
	/*----------------------------------------------*/
	
	private void publishUpdatedTopology() {
		JsonObject toStorageMsg = new JsonObject()
				.put("action", "get_topology")
				.put("params", new JsonObject());

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();				
				if (response.containsKey("content")) {
					JsonObject ebPubMsg = new JsonObject()
							.put("service", serviceName)
							.put("content", response.getJsonObject("content"));
					eb.publish("nms.info.topology", ebPubMsg);					
				} else {
					LOG.error("Cannot get updated topology", response.getString("error"));
				}
			} else {
				LOG.error("Cannot get updated topology", reply.cause().getMessage());
			}
		});
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[Topology] Closing Service.");
		super.stop(stopFuture);
	}
}
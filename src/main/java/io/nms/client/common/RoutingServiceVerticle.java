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

public class RoutingServiceVerticle extends AmqpVerticle {
	
	private static final int TOPO_UPDATE_PERIOD_MS = 10000;
	private static final int RESET_PERIOD_S = 60;
	private static final int SPEC_PERIOD_MS = 5000;
	
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected Instant lastUpdate = Instant.now();
	
	public void start(Future<Void> fut) {
		serviceName = "nms.routing";
		Future<Void> futBase = Future.future(promise -> super.start(promise));
		futBase.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				setTopologyListener();
				// update topo every 60s
				/*vertx.setPeriodic(TOPO_UPDATE_PERIOD_MS, id -> {
					LOG.info("Check for new routing capabilities");
					if (lastUpdate.plusSeconds(RESET_PERIOD_S).isBefore(Instant.now())) {
						LOG.info("Reset discovered capabilities");
						knownCaps.clear();
						lastUpdate = Instant.now();
					}
					Future<List<Capability>> futCaps = Future
						.future(promise -> getRoutingCapabilities(promise));
					futCaps.setHandler(newCapsRes -> {
						if (newCapsRes.failed()) {
							fut.fail(newCapsRes.cause());
						} else {
							if (!newCapsRes.result().isEmpty()) {
								LOG.info("New routing capabilities have been found.");
							}
						}
					});
				});*/
				fut.complete();
			}
		});
	}
	
	// routing listens to updates from topology service
	protected void setTopologyListener() {
		eb.consumer("nms.info.topology", message -> {
			NmsEbMessage nmsEbMsg = new NmsEbMessage(message);
			LOG.info("[" + serviceName + "] got topology update.");
			// TODO: update links and nodes...
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

			case "get_all_reg_pref":
				getAllRegPref(nmsEbMsg);
				break;
			case "get_reg_pref":
				getRegPref(nmsEbMsg);
				break;
			case "get_all_routes":
				getAllRoutes(nmsEbMsg);
				break;
			case "get_route":
				getRoute(nmsEbMsg);
				break;
				
			case "add_reg_pref":
				addRegPref(nmsEbMsg);
				break;
			case "add_route":
				addRoute(nmsEbMsg);
				break;
				
			case "del_reg_pref":
				deleteRegPref(nmsEbMsg);
				break;
			case "del_reg_pref_by_node":
				deleteRegPrefByNode(nmsEbMsg);
				break;
			case "del_route":
				deleteRoute(nmsEbMsg);
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
			LOG.info("publish routing result.");
			// ...
		}
	}
	
	/*---------- topology service processing ---------*/
	protected void getRoutingCapabilities(Future<List<Capability>> future) {
		List<Capability> newCaps = new ArrayList<Capability>();
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	for (Capability c : res.result()) {
	        		if (!c.getName().equals("routing")) {
	        			continue;
	        		} else if (!knownCaps.containsKey(c.getAgentId())) {
						knownCaps.put(c.getAgentId(), c);
						newCaps.add(c);
					}
				}
	        	future.complete(newCaps);
	        } else {
	        	LOG.error("Failed to update routing capabilities", res.cause());
	        	future.fail(res.cause());
	        }
		});
	}
	
	protected void sendRoutingSpecifications(List<Capability> caps) {
		/* TODO: update ... */
		/*for (Capability c : caps) {	
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
		}*/
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
	
	protected void getAllRegPref(NmsEbMessage message) {
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_all_prefixes")
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
	
	protected void getRegPref(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Prefix Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_prefix")
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
	
	protected void getAllRoutes(NmsEbMessage message) {
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_all_routes")
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
	
	protected void getRoute(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Route Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "get_route")
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
	
	protected void addRegPref(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("name","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "prefix must have a name");
			message.reply(response);
			return;
		}
		if (params.getString("node","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Missing node Id");
			message.reply(response);
			return;
		}
		
		JsonObject checkNodeMsg = new JsonObject()
				.put("action", "get_node")
				.put("params", new JsonObject().put("_id", params.getString("node")));

		eb.send("nms.storage", checkNodeMsg, reply1 -> {
			if (reply1.succeeded()) {
				JsonObject nodeResp = (JsonObject)reply1.result().body();
				if (nodeResp.containsKey("content")) {
					if (!nodeResp.getJsonObject("content").isEmpty()) {
						params.put("status", "pending");
						
						JsonObject addPrefMsg = new JsonObject()
								.put("action", "add_prefix")
								.put("params", params);

						eb.send("nms.storage", addPrefMsg, reply -> {
							if (reply.succeeded()) {
								JsonObject response = (JsonObject)reply.result().body();
								response.put("service", serviceName);
								response.put("action", message.getAction());
								message.reply(response);
								publishUpdatedPrefixes();
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
						response.put("error", "specified node does not exist");
						message.reply(response);
					}
				} else {
					JsonObject response = new JsonObject();
					response.put("service", serviceName);
					response.put("action", message.getAction());
					response.put("error", "failed to check node existence");
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
	
	protected void addRoute(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("prefix","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Prefix Id missing");
			message.reply(response);
			return;
		}
		if (params.getString("fromNode","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "fromNode missing");
			message.reply(response);
			return;
		}
		if (params.getJsonArray("path", new JsonArray()).isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Path cannot be empty");
			message.reply(response);
			return;
		}
		
		// check prefix existence
		Future<Void> getPrefFut = Future.future();
		JsonObject getPrefMsg = new JsonObject()
				.put("action", "get_prefix")
				.put("params", new JsonObject().put("_id", params.getString("prefix")));
		eb.send("nms.storage", getPrefMsg, rep -> {
			if (rep.succeeded()) {
				JsonObject getPrefResp = (JsonObject)rep.result().body();
				if (getPrefResp.containsKey("content")) {
					if (!getPrefResp.getJsonObject("content").isEmpty()) {
						getPrefFut.complete();
					} else {
						getPrefFut.fail("prefix does not exist");
					}
				} else {
					getPrefFut.fail(getPrefResp.getString("error"));
				}
			} else {
				getPrefFut.fail(rep.cause());
			}
		});
		
		// check nodes existence
		Future<Void> getNodesFut = Future.future();
		JsonArray nodes = params.getJsonArray("path");
		//nodes.add(params.getString("fromNode"));
		JsonObject getNodesMsg = new JsonObject()
				.put("action", "get_nodes")
				.put("params", new JsonObject().put("nodes", nodes));

		eb.send("nms.storage", getNodesMsg, rep -> {
			if (rep.succeeded()) {
				JsonObject getNodesResp = (JsonObject)rep.result().body();
				if (getNodesResp.containsKey("content")) {
					int d = getNodesResp.getJsonObject("content").getJsonArray("docs").size();
					if (d == nodes.size()) {
						getNodesFut.complete();
					} else {
						getNodesFut.fail("one or many specified nodes do not exist");
					}
				} else {
					getNodesFut.fail(getNodesResp.getString("error"));
				}
			} else {
				getNodesFut.fail(rep.cause());
			}
		});
		
		// add route
		CompositeFuture.all(getPrefFut, getNodesFut).setHandler(ar -> {
			if (ar.failed()) {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", ar.cause().getMessage());
				message.reply(response);
			} else {
				params.put("status", "pending");
				JsonObject addRouteMsg = new JsonObject()
						.put("action", "add_route")
						.put("params", params);

				eb.send("nms.storage", addRouteMsg, reply -> {
					if (reply.succeeded()) {
						JsonObject response = (JsonObject)reply.result().body();
						response.put("service", serviceName);
						response.put("action", message.getAction());
						message.reply(response);
						publishUpdatedRoutes();
					} else {
						JsonObject response = new JsonObject();
						response.put("service", serviceName);
						response.put("action", message.getAction());
						response.put("error", reply.cause().getMessage());
						message.reply(response);
					}
				});
			}
		});
	}
	
	protected void deleteRegPref(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Prefix Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "del_prefix")
			.put("params", params);

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				message.reply(response);
				publishUpdatedPrefixes();
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply.cause().getMessage());
				message.reply(response);
				}
		});
	}
	
	protected void deleteRegPrefByNode(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Node Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "del_prefix_by_node")
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
	
	protected void deleteRoute(NmsEbMessage message) {
		JsonObject params = message.getParams();
		if (params.getString("_id","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Route Id not specified");
			message.reply(response);
			return;
		}
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "del_route")
			.put("params", params);

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				message.reply(response);
				publishUpdatedRoutes();
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply.cause().getMessage());
				message.reply(response);
				}
		});
	}
	
	private void publishUpdatedPrefixes() {
		JsonObject toStorageMsg = new JsonObject()
				.put("action", "get_all_prefixes")
				.put("params", new JsonObject());

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();				
				if (response.containsKey("content")) {
					JsonObject ebPubMsg = new JsonObject()
							.put("service", serviceName)
							.put("content", response.getJsonObject("content"));
					eb.publish("nms.info.routing.prefixes", ebPubMsg);					
				} else {
					LOG.error("Cannot get updated prefixes", response.getString("error"));
				}
			} else {
				LOG.error("Cannot get updated prefixes", reply.cause().getMessage());
			}
		});
	}
	
	private void publishUpdatedRoutes() {
		JsonObject toStorageMsg = new JsonObject()
				.put("action", "get_all_routes")
				.put("params", new JsonObject());

		eb.send("nms.storage", toStorageMsg, reply -> {
			if (reply.succeeded()) {
				JsonObject response = (JsonObject)reply.result().body();				
				if (response.containsKey("content")) {
					JsonObject ebPubMsg = new JsonObject()
							.put("service", serviceName)
							.put("content", response.getJsonObject("content"));
					eb.publish("nms.info.routing.routes", ebPubMsg);					
				} else {
					LOG.error("Cannot get updated routes", response.getString("error"));
				}
			} else {
				LOG.error("Cannot get updated routes", reply.cause().getMessage());
			}
		});
	}
	/*----------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("Closing "+serviceName+" Service.");
		super.stop(stopFuture);
	}
}
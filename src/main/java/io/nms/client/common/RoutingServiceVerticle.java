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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class RoutingServiceVerticle extends AmqpVerticle {
	
	private static final int TOPO_UPDATE_PERIOD_MS = 10000;
	private static final int RESET_PERIOD_S = 60;
	private static final int SPEC_PERIOD_MS = 5000;
	
	protected String serviceName = "nms.routing";
	
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected Instant lastUpdate = Instant.now();
	
	public void start(Future<Void> fut) {
		Future<Void> futBase = Future.future(promise -> super.start(promise));
		futBase.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				setTopologyListener();
				// update topo every 60s
				vertx.setPeriodic(TOPO_UPDATE_PERIOD_MS, id -> {
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
				});
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
				response.put("error", reply.cause());
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
				response.put("error", reply.cause());
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
				response.put("error", reply.cause());
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
				response.put("error", reply.cause());
				message.reply(response);
			}
		});
	}
	
	protected void addRegPref(NmsEbMessage message) {
		/* TODO: check node id existence */
		JsonObject params = message.getParams();
		if (params.getString("name","").isEmpty()) {
			JsonObject response = new JsonObject();
			response.put("service", serviceName);
			response.put("action", message.getAction());
			response.put("error", "Prefix must have a name");
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
		params.put("status", "pending");
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "add_prefix")
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
				response.put("error", reply.cause());
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
			response.put("error", "Prefix missing");
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
		params.put("status", "pending");
		
		JsonObject toStorageMsg = new JsonObject()
			.put("action", "add_route")
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
				response.put("error", reply.cause());
				message.reply(response);
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
			} else {
				JsonObject response = new JsonObject();
				response.put("service", serviceName);
				response.put("action", message.getAction());
				response.put("error", reply.cause());
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
		LOG.info("Closing "+serviceName+" Service.");
		super.stop(stopFuture);
	}
}
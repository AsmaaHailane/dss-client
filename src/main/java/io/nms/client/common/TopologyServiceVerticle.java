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

public class TopologyServiceVerticle extends AmqpVerticle {
	
	private static final int TOPO_UPDATE_PERIOD_MS = 20000;
	private static final int RESET_PERIOD_S = 600;
	private static final int SPEC_PERIOD_MS = 10000;
	
	protected String serviceName = "nms.topology";
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected Instant lastUpdate = Instant.now();
	
	protected HashMap<String, JsonObject> nodes = new HashMap<String, JsonObject>();
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
				message.reply("");
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
			/*long stopTime = Instant.now().plusSeconds(RESET_PERIOD_S).toEpochMilli();
			c.setWhen("now ... "+String.valueOf(stopTime)+" / "+SPEC_PERIOD_MS);
			JsonObject capability = new JsonObject(Message.toJsonString(c, false));
			JsonObject params = new JsonObject()
				.put("capability", capability);
			
			// use DSS
			JsonObject ebMsg = new JsonObject()
				.put("action", "send_specification")
				.put("params", params);
			eb.send("nms.dss", ebMsg, reply -> {
				if (reply.succeeded()) {
					JsonObject response = (JsonObject) reply.result().body();
					if (response.containsKey("content")) {
						JsonObject receipt = response
							.getJsonObject("content")
							.getJsonObject("receipt");
						LOG.info("Rct: "+receipt.encodePrettily());
					}
				} else {
					LOG.error("Failed to get Receipt", reply.cause());
				}
			});*/
			
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
		LOG.info("update topology with result from: "+res.getAgentId());
		
		// support new nodes and links
		/*boolean newNode = false;
		if (!nodes.containsKey(res.getAgentId())) {
			JsonObject node = new JsonObject()
					.put("id",res.getAgentId())
					.put("group", 1);
			nodes.put(res.getAgentId(), node);
			newNode = true;
		}
		if (newNode) {
			int targetIndex = res.getResults().indexOf("toaddress");
			List<List<String>> nodeTopo = res.getResultValues();
			for (List<String> c : nodeTopo) {
				JsonObject link = new JsonObject()
						.put("source", res.getAgentId())
						.put("target", c.get(targetIndex))
						.put("value", 1);
				links.add(link);
			}
		}*/
		
		// support update links for existent nodes
		//...
		
		future.complete();
	}
	/*------------------------------------------------*/
	
	/*--------------- API functions ----------------*/
	
	protected void getTopology(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("content", new JsonObject());
		message.reply(response);
	}
	/*----------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[Topology] Closing Service.");
		super.stop(stopFuture);
	}
}
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
	private static final int RESET_PERIOD_S = 300;
	private static final int SPEC_PERIOD_MS = 30000;
	
	protected String serviceName = "nms.topology";
	protected HashMap<String, Capability> knownCaps = new HashMap<String, Capability>();
	protected JsonObject currentTopology = new JsonObject();
	protected Instant lastUpdate = Instant.now();	
	
	public void start(Future<Void> fut) {
		Future<Void> futBase = Future.future(promise -> super.start(promise));
		futBase.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				// update topo every 60s
				vertx.setPeriodic(TOPO_UPDATE_PERIOD_MS, id -> {
					LOG.info("Update topology");
					// reset known capabilities every 10mn
					if (lastUpdate.plusSeconds(RESET_PERIOD_S).isBefore(Instant.now())) {
						LOG.info("Clear Capabilities");
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
								sendTopologySpecifications(newCapsRes.result());
							} else {
								LOG.info("No new topology capabilities");
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
					JsonObject ebPubMsg = new JsonObject()
						.put("service", serviceName)
						.put("content", currentTopology);
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
	        		}
					if (!knownCaps.containsKey(c.getAgentId())) {
						knownCaps.put(c.getAgentId(), c);
						newCaps.add(c);
					} 
					/*else if (!knownCaps.get(c.getAgentId()).getSchema().equals(c.getSchema())) {
						knownCaps.put(c.getAgentId(), c);
						newCaps.add(c);
					}*/
				}
	        	future.complete(newCaps);
	        } else {
	        	LOG.error("Failed to update topoloy capabilities", res.cause());
	        	future.fail(res.cause());
	        }
		});
	}
	
	protected void sendTopologySpecifications(List<Capability> caps) {
		for (Capability c : caps) {
			Specification spec = new Specification(c);
			// specification will stop after 10mn, knownCaps is cleared every 10mn
			long stopTime = Instant.now().plusSeconds(RESET_PERIOD_S).toEpochMilli();
			spec.setWhen("now ... "+String.valueOf(stopTime)+" / "+SPEC_PERIOD_MS);
			spec.setTimestampNow();
			
			LOG.info("Spec: "+Message.toJsonString(spec, true));
			
			Future<Receipt> rct = Future.future(promise -> sendSpecification(spec, promise));
			rct.setHandler(res -> {
				if (res.succeeded()) {
					LOG.info("Spec: "+Message.toJsonString(res.result(), true));
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
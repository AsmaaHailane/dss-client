package io.nms.client.common;

import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class TopologyServiceVerticle extends AmqpVerticle {
	
	protected String serviceName = "nms.topology";
	
	public void start(Future<Void> fut) {
		super.start(fut);
	}
	
	@Override
	protected void setServiceApi() {
		eb.consumer("nms.topology", message -> {
			JsonObject query = ((JsonObject) message.body());
			String action = query.getString("action");
			JsonObject params = query.getJsonObject("params");
			LOG.info("[" + serviceName + "] got query: " + action + " | " + params.encodePrettily());
			
			// TODO: check query...
			
			switch (action)
			{
			case "get.serviceinfo":
				getServiceInfo(message);
				break;	
			case "get.topology":
				getTopology(message);
				break;

			default:
				message.reply("");
			}
		});
	}
	
	@Override
	protected void processResult(io.nms.messages.Message res) {
		// check if result expected
		if (activeSpecs.containsKey(res.getSchema())) {
			String resStr = io.nms.messages.Message.toJsonString(res, false);
			LOG.info("Got Result: "+resStr);
		
			// send over eventbus
			eb.send("nms.topology", resStr);
		
			// send to storage (TODO: if defined)
			/*JsonObject message = new JsonObject()
				.put("action", "add_result")
				.put("payload", new JsonObject(resStr));
			eb.send("dss.storage", message, reply -> {
				if (reply.succeeded()) {
					LOG.info("Result stored.");
				}
			});*/
		}
	}
	
	/*--------------- API functions ----------------*/
	protected void getServiceInfo(Message<Object> message) {
		JsonObject clientId = new JsonObject()
			.put("name", serviceName)
	        .put("role", clientRole);
		message.reply(new JsonObject().put("client", clientId));	      
	}
	
	protected void getTopology(Message<Object> message) {
		message.reply("");
	}
	/*----------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[Topology] Closing Service.");
		super.stop(stopFuture);
	}
}
package io.nms.client.common;

import io.nms.storage.NmsEbMessage;
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
	protected void processResult(io.nms.messages.Message res) {
		// check if result expected
		if (activeSpecs.containsKey(res.getSchema())) {
			String resStr = io.nms.messages.Message.toJsonString(res, false);
			LOG.info("Got Result: "+resStr);
			
			// some processing...
			
			// send over eventbus
			//eb.send("nms.topology", resStr);
		
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
	protected void getServiceInfo(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		JsonObject content = new JsonObject()
			.put("service_name", serviceName)
			.put("complete_name", clientName)
	        .put("role", clientRole);
		response.put("content", content);
		
		message.reply(response);	      
	}
	
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
package io.nms.client.common;

import java.util.List;

import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.nms.storage.NmsEbMessage;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DataServiceVerticle extends AmqpVerticle {
	
	public void start(Future<Void> fut) {	
		super.start(fut);
		serviceName = "nms.dss";
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
			case "get_capabilities":
				getCapabilities(nmsEbMsg);
				break;		
			case "send_specification":
				sendSpecification(nmsEbMsg);
				break;
			case "send_interrupt":
				sendInterrupt(nmsEbMsg);
				break;
			
			default:
				unsupportedAction(nmsEbMsg);
			}
		});
	}
	
	@Override
	protected void processResult(io.nms.messages.Message res) {
		// check if result expected
		if (activeSpecs.containsKey(res.getSchema())) {
			String resStr = io.nms.messages.Message.toJsonString(res, false);
			LOG.info("Got Result: "+resStr);
		
			// TODO: publish nms.info.dss
			JsonObject ebPubMsg = new JsonObject()
				.put("service", serviceName)
				.put("content", new JsonObject(resStr));
			eb.publish("nms.info.dss", ebPubMsg);
		
			// send to storage
			JsonObject message = new JsonObject()
				.put("action", "add_result")
				.put("params", new JsonObject(resStr));
			eb.send("nms.storage", message, reply -> {
				if (reply.succeeded()) {
					JsonObject opResult = (JsonObject)reply.result().body();
					if (!opResult.containsKey("error")) {
						LOG.info("Result stored.");
					}
				}
			});
		}
	}
	
	/*--------------- API functions ----------------*/
	
	protected void getCapabilities(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		JsonObject params = message.getParams();
		// TODO: check params...
		
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	String caps = io.nms.messages.Message.toStringFromList(res.result());
	        	JsonObject content = new JsonObject().put("docs", new JsonArray(caps));
	        	response.put("content", content);
	        	message.reply(response);
	        } else {
	        	LOG.error("Failed to get Capabilities", res.cause());
	        	response.put("error", "internal error");
	        	message.reply(response);
	        }
		});
	}
	protected void sendSpecification(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		JsonObject params = message.getParams();
		// TODO: check params...
		
		if (!params.containsKey("capability")) {
			response.put("error", "missing capabilitiy");
        	message.reply(response);
			return;
		}
		
		String strCap = params.getJsonObject("capability").encode();
		Capability cap = (Capability) io.nms.messages.Message.fromJsonString(strCap);
		Specification spec = new Specification(cap);
		Future<Receipt> fut = Future.future(rct -> sendSpecification(spec, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	String receipt = io.nms.messages.Message.toJsonString(res.result(), false);
	        	JsonObject content = new JsonObject();
	        	content.put("receipt", new JsonObject(receipt));
	        	response.put("content", content);
	        	message.reply(response);
	        } else {
	        	LOG.error("Failed to get Receipt", res.cause());
	        	response.put("error", "internal error");
	        	message.reply(response);
	        }
	    });
	}
	protected void sendInterrupt(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		JsonObject params = message.getParams();
		// TODO: check params...
		
		if (!params.containsKey("receipt")) {
			response.put("error", "missing receipt");
        	message.reply(response);
			return;
		}
		
		String rctSchema = params.getString("receipt");
		if (activeSpecs.containsKey(rctSchema)) {
			Interrupt interrupt = new Interrupt(activeSpecs.get(rctSchema));
			LOG.info("Send Interrupt: "+io.nms.messages.Message.toJsonString(interrupt, true));
			Future<Receipt> fut = Future.future(rct -> sendInterrupt(interrupt, rct));
			fut.setHandler(res -> {
				if (res.succeeded()) {
					activeSpecs.remove(rctSchema);
					String receipt = io.nms.messages.Message.toJsonString(res.result(), false);
		        	JsonObject content = new JsonObject();
		        	content.put("receipt", new JsonObject(receipt));
		        	response.put("content", content);
		        	message.reply(response);
				} else {
					LOG.error("Failed to get Receipt", res.cause());
					response.put("error", "internal error");
		        	message.reply(response);
				}
			});
		} else {
			response.put("error", "specification not found");
        	message.reply(response);
		}
	}
	
	protected void unsupportedAction(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("error", "Action unsupported");		
		message.reply(response);
	}
	
	/*----------------------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[DSS] Closing Service.");
		super.stop(stopFuture);
	}
}
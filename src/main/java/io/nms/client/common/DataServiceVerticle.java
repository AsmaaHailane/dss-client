package io.nms.client.common;

import java.util.List;

import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DataServiceVerticle extends AmqpVerticle {
	
	protected String serviceName = "nms.dss";
	
	public void start(Future<Void> fut) {
		super.start(fut);
	}
	
	@Override
	protected void setServiceApi() {
		eb.consumer("nms.dss", message -> {
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
			case "get.capabilities":
				getCapabilities(message, params);
				break;		
			case "send.specification":
				sendSpecification(message, params);
				break;
			case "send.interrupt":
				sendInterrupt(message, params);
				break;
			
			// result read/delete
			case "get_all_operations":
				toResultsStorageAPI(message); 
				break;
			case "get_results_operation":
				toResultsStorageAPI(message); 
				break;
			case "get_result_id":
				toResultStorageAPI(message); 
				break;
			case "del_results_operation":
				toResultStorageAPI(message); 
				break;
			case "del_result_id":
				toResultStorageAPI(message); 
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
			eb.send("nms.dss", resStr);
		
			// send to storage (TODO: if defined)
			JsonObject message = new JsonObject()
				.put("action", "add_result")
				.put("payload", new JsonObject(resStr));
			eb.send("dss.storage", message, reply -> {
				if (reply.succeeded()) {
					LOG.info("Result stored.");
				}
			});
		}
	}
	
	/*--------------- API functions ----------------*/
	protected void getServiceInfo(Message<Object> message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		JsonObject content = new JsonObject()
			.put("service_name", serviceName)
			.put("complete_name", clientName)
	        .put("role", clientRole);
		response.put("content", content);
		
		message.reply(response);	      
	}
	
	protected void getCapabilities(Message<Object> message, JsonObject params) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
		// TODO: use params (match...)
		
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	String caps = io.nms.messages.Message.toStringFromList(res.result());
	        	JsonObject content = new JsonObject().put("capabilities", caps);
	        	response.put("content", content);
	        	message.reply(response);
	        } else {
	        	LOG.error("Failed to get Capabilities", res.cause());
	        	response.put("error", "internal error");
	        	message.reply(response);
	        }
		});
	}
	protected void sendSpecification(Message<Object> message, JsonObject params) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
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
	        	JsonObject content = new JsonObject();
	        	content.put("receipt", io.nms.messages.Message.toJsonString(res.result(), false));
	        	response.put("content", content);
	        	message.reply(response);
	        } else {
	        	LOG.error("Failed to get Receipt", res.cause());
	        	response.put("error", "internal error");
	        	message.reply(response);
	        }
	    });
	}
	protected void sendInterrupt(Message<Object> message, JsonObject params) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		
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
					JsonObject content = new JsonObject();
					content.put("receipt", io.nms.messages.Message.toJsonString(res.result(), false));
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
	
	/*----------------------------------------------------------*/
	protected void toResultsStorageAPI(Message<Object> message) {
    	JsonObject toStorageMsg = new JsonObject()
    		.put("action", message.headers().get("action"))
    		.put("payload", (JsonObject) message.body());
    	eb.send("dss.storage", toStorageMsg, reply -> {
    		if (reply.succeeded()) {
    			LOG.info( ((JsonArray)reply.result().body()).encodePrettily());
    			message.reply((JsonArray)reply.result().body());
    		} else {
    			LOG.error("get_all_operations failed.", reply.cause());
	        	message.reply(new JsonArray());
    		}
    	});
	}
	protected void toResultStorageAPI(Message<Object> message) {
		// to customMessage format...
    	JsonObject toStorageMsg = new JsonObject()
    		.put("action", message.headers().get("action"))
    		.put("payload", (JsonObject) message.body());
    	eb.send("dss.storage", toStorageMsg, reply -> {
    		if (reply.succeeded()) {
    			message.reply((JsonObject)reply.result().body());
    			//LOG.info(((JsonObject)reply.result()).encodePrettily());
    		} else {
    			LOG.error("get_all_operations failed.", reply.cause());
	        	message.reply(new JsonObject());
    		}
    	});
	}
	/*----------------------------------------------*/
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("[DSS] Closing Service.");
		super.stop(stopFuture);
	}
}
	/*
	protected void getAgents(Message<Object> message) {
		JsonObject req = new JsonObject();
    	req.put("action", "get_all_agents");
    	req.put("payload", new JsonObject());
    	Future<String> fut = Future
    		.future(promise -> sendAdminReq(req, promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	JsonObject r = new JsonObject(res.result());
	        	message.reply(new JsonObject().put("agents", r.getJsonArray("payload")));
	        } else {
	        	LOG.error("Failed to get Agents", res.cause());
	        	message.reply(new JsonObject().put("agents", new JsonObject()));
	        }
	    });
	}
	protected void getClients(Message<Object> message) {
		JsonObject req = new JsonObject();
    	req.put("action", "get_all_clients");
    	req.put("payload", new JsonObject());
    	Future<String> fut = Future
    			.future(promise -> sendAdminReq(req, promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	JsonObject r = new JsonObject(res.result());
	        	message.reply(new JsonObject().put("clients", r.getJsonArray("payload")));
	        } else {
	        	LOG.error("Failed to get Clients", res.cause());
	        	message.reply(new JsonObject().put("clients", new JsonObject()));
	        }
	    });
	}
	protected void getStats(Message<Object> message) {
		JsonObject req = new JsonObject();
    	req.put("action", "get_stats");
    	req.put("payload", new JsonObject());
    	Future<String> fut = Future
    			.future(promise -> sendAdminReq(req, promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	JsonObject r = new JsonObject(res.result());
	        	message.reply(new JsonObject().put("stats", r));
	        } else {
	        	LOG.error("Failed to get Clients", res.cause());
	        	message.reply(new JsonObject().put("stats", new JsonObject()));
	        }
	    });
	} */

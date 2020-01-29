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
	
	public void start(Future<Void> fut) {
		super.start();
		
		String uname = config().getJsonObject("client").getString("username", "");
		String pass = config().getJsonObject("client").getString("password", "");
	
		String host = config().getJsonObject("amqp").getString("host", "");
		int port = config().getJsonObject("amqp").getInteger("port", 0);
		
		Future<Void> futEb = Future.future(promise -> initEventBus(promise));
		Future<Void> futConn = futEb
			.compose(v -> {
				return Future.<Void>future(promise -> createAmqpConnection(host, port, promise));
			})
			.compose(v -> {
				return Future.<Void>future(promise -> requestAuthentication(uname, pass, promise));
			});
		futConn.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				setServiceApi();
				fut.complete();
			}
		});
	}
	
	@Override
	protected void setServiceApi() {
		eb.consumer("nms.dss", message -> {
			JsonObject msgBody = ((JsonObject) message.body());
			String action = msgBody.getString("action");
			LOG.info("[DSS] EB message: " + action + " | " + msgBody.getJsonObject("payload").encodePrettily());
			
			switch (action)
			{
			case "get.serviceinfo":
				getServiceInfo(message);
				break;	
			case "get.capabilities":
				getCapabilities(message);
				break;		
			case "send.specification":
				sendSpecification(message);
				break;
			case "send.interrupt":
				sendInterrupt(message);
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
		
			// send to ResultListener (if defined)
			if (this.rListener != null) {
				this.rListener.onResult(res);
			}
		
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
		JsonObject clientId = new JsonObject()
			.put("name", clientName)
	        .put("role", clientRole);
		message.reply(new JsonObject().put("client", clientId));	      
	}
	
	protected void getCapabilities(Message<Object> message) {
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	String caps = io.nms.messages.Message.toStringFromList(res.result());
	        	JsonObject json = new JsonObject().put("capabilities", caps);
	        	message.reply(json);	      
	        } else {
	        	LOG.error("Failed to get Capabilities", res.cause());
	        	message.reply(new JsonObject().put("capabilities", new JsonObject()));
	        }
		});
	}
	protected void sendSpecification(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body());
		String strCap = payload.getString("capability");
		Capability cap = (Capability) io.nms.messages.Message.fromJsonString(strCap);
		Specification spec = new Specification(cap);
		//LOG.info("Send Specification: "+io.nms.messages.Message.toJsonString(spec, true));
		Future<Receipt> fut = Future.future(rct -> sendSpecification(spec, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	message.reply(io.nms.messages.Message.toJsonString(res.result(), false));
	        } else {
	        	LOG.error("Failed to get Receipt", res.cause());
	        	message.reply("");
	        }
	    });
	}
	protected void sendInterrupt(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body());
		String rctSchema = payload.getString("receipt.schema");
		if (activeSpecs.containsKey(rctSchema)) {
			Interrupt interrupt = new Interrupt(activeSpecs.get(rctSchema));
			LOG.info("Send Interrupt: "+io.nms.messages.Message.toJsonString(interrupt, true));
			Future<Receipt> fut = Future.future(rct -> sendInterrupt(interrupt, rct));
			fut.setHandler(res -> {
				if (res.succeeded()) {
					activeSpecs.remove(rctSchema);
					message.reply(io.nms.messages.Message.toJsonString(res.result(), false));	        	
				} else {
					LOG.error("Failed to get Receipt", res.cause());
					message.reply("");
				}
			});
		}
	}
	protected void toResultsStorageAPI(Message<Object> message) {
		// to customMessage format...
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

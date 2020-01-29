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

public class TopologyServiceVerticle extends AmqpVerticle {
	
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
		eb.consumer("nms.topology", message -> {
			JsonObject msgBody = ((JsonObject) message.body());
			String action = msgBody.getString("action");
			LOG.info("[Topology] EB message: " + action + " | " + msgBody.getJsonObject("payload").encodePrettily());
			
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
		
			// send to ResultListener (if defined)
			if (this.rListener != null) {
				this.rListener.onResult(res);
			}
		
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
			.put("name", clientName)
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
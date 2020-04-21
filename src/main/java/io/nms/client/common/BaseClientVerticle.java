package io.nms.client.common;

import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.nms.storage.NmsEbMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;


public abstract class BaseClientVerticle extends AbstractVerticle {
	protected Logger LOG = LoggerFactory.getLogger(BaseClientVerticle.class);
	
	protected String serviceName = "";
	protected String clientName = "";
	protected String clientRole = "";
	
	protected int msgNbr = 0;
	protected String status = "running";
	
	protected HashMap<String, Receipt> activeSpecs = new HashMap<String, Receipt>();	
	
	EventBus eb = null;
	
	// implemented by AmqpVerticle
	protected abstract void createAmqpConnection(String host, int port, Future<Void> promise);
	protected abstract void requestAuthentication(String uname, String pass, Future<Void> prom);
	protected abstract void subscribeToResults(Receipt rct, Future<Void> prom);
	protected abstract void discoverCapabilities(Future<List<Capability>> prom);
	protected abstract void sendSpecification(Specification spec, Future<Receipt> prom);
	protected abstract void sendInterrupt(Interrupt itr, Future<Receipt> prom); 
	protected abstract void sendAdminReq(JsonObject req, Future<String> promise);
		
	// implemented by XxxServiceVerticle
	protected abstract void processResult(io.nms.messages.Message fromJsonString);
	protected abstract void setServiceApi();

	@Override
	public void start(Future<Void> fut) {
		String uname = config().getJsonObject("client").getString("username", "");
		String pass = config().getJsonObject("client").getString("password", "");
	
		String host = config().getJsonObject("amqp").getString("host", "");
		int port = config().getJsonObject("amqp").getInteger("port", 0);
		
		Future<Void> futConn = Future.future(promise -> createAmqpConnection(host, port, promise));
		Future<Void> futAuth = futConn
			.compose(v -> {
				return Future.<Void>future(promise -> requestAuthentication(uname, pass, promise));
			});
		futAuth.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				eb = vertx.eventBus();
				setServiceApi();
				fut.complete();
			}
		});
	}
	
	protected void getServiceInfo(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("action", message.getAction());
		
		JsonObject content = new JsonObject()
			.put("name", clientName)
	        .put("role", clientRole)
			.put("status", status)
			.put("messages", msgNbr);
		response.put("content", content);
		message.reply(response);	      
	}
	
	protected void publishLogging(String message) {
		Timestamp ts = new Timestamp(new Date().getTime());
		JsonObject content = new JsonObject()
				.put("timestamp", ts.toString())
				.put("message", message);
		JsonObject ebPubMsg = new JsonObject()
				.put("service", serviceName)
				.put("content", content);
		eb.publish("nms.logging", ebPubMsg);		
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		super.stop(stopFuture);
	}
	
}

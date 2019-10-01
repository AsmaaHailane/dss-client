package io.nms.client.common;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nms.client.cli.ResultListener;
import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.CorsHandler;


public abstract class BaseClientVerticle extends AbstractVerticle {
	protected Logger LOG = LoggerFactory.getLogger(BaseClientVerticle.class);
	protected ResultListener rListener = null;
	
	protected abstract void requestAuthentication(String uname, String pass, Future<Void> prom);
	protected abstract void subscribeToResults(Receipt rct, Future<Void> prom);
	public abstract void discoverCapabilities(Future<List<Capability>> prom);
	public abstract void sendSpecification(Specification spec, Future<Receipt> prom);
	public abstract void sendInterrupt(Interrupt itr, Future<Receipt> prom); 
	public abstract void discoverUsers(Future<JsonArray> promise, String type);
	

	@Override
	public void start() {
		
		EventBus eb = vertx.eventBus();
		
		Router router = Router.router(vertx);
		BridgeOptions opts = new BridgeOptions()
				.addOutboundPermitted(new PermittedOptions().setAddress("client"))
				.addInboundPermitted(new PermittedOptions().setAddress("client"));
		SockJSHandler ebHandler = SockJSHandler.create(vertx).bridge(opts);
		
		Set<String> allowedHeaders = new HashSet<>();
		allowedHeaders.add("Access-Control-Allow-Origin");
		
		CorsHandler ch = CorsHandler.create("http://10.11.200.213:8080").allowedHeaders(allowedHeaders)
				.allowCredentials(false);
		
		
		router.route().handler(ch);
		router.route("/eventbus/*").handler(ebHandler);

		eb.consumer("client", message -> {
			LOG.info("got EB message");
			JsonObject body = (JsonObject) message.body();
			String action = body.getString("action");
			
			switch (action)
			{
			case "get.capabilities":
				getCapabilities(message);
				break;		

			case "send.specification":
				sendSpecification(message);
				break;
				
			case "send.interrupt":
				sendInterrupt(message);
				break;
				
			case "get.users":
				getUsers(message);
				break;

			default:
				message.reply("");
			}
		});
	}
	
	protected void getCapabilities(Message<Object> message) {
		//JsonObject payload = ((JsonObject) message.body()).getJsonObject("payload");
		Future<List<Capability>> fut = Future.future(promise -> discoverCapabilities(promise));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	message.reply(res.result());	      
	        } else {
	        	message.reply("");
	        }
		});
	}
	
	protected void getUsers(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body()).getJsonObject("payload");
		Future<JsonArray> fut = Future.future(promise -> discoverUsers(promise, payload.getString("type") ));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	message.reply(res.result());	      
	        } else {
	        	message.reply("");
	        }
		});
	}
	
	protected void sendSpecification(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body()).getJsonObject("payload");
		
		String strCap = payload.getString("capability");
		int duration = payload.getInteger("duration");
		int period = payload.getInteger("period");
		Capability cap = (Capability) io.nms.messages.Message.fromJsonString(strCap);
		Specification spec = new Specification(cap);
		
		long stop = Instant.now().plusSeconds(duration).toEpochMilli();
		String when = "now ... " + String.valueOf(stop) + " / " + period;
		spec.setWhen(when);
		
		Future<Receipt> fut = Future.future(rct -> sendSpecification(spec, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	message.reply(res.result());	        	
	        } else {
	        	message.reply("");
	        }
	    });
	}
	
	protected void sendInterrupt(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body()).getJsonObject("payload");
		
		Receipt receipt = (Receipt) io.nms.messages.Message.fromJsonString(payload.getString("receipt"));
		
		Interrupt interrupt = new Interrupt(receipt);
		String taskId = receipt.getContent("task.id");
		interrupt.setParameter("task.id", taskId);	
		Future<Receipt> fut = Future.future(rct -> sendInterrupt(interrupt, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {	    	 
	        	message.reply(res.result());	        	
	        } else {
	        	message.reply("");
	        }
	    });
	}
	
	protected void processResult(io.nms.messages.Message res) {
		if (this.rListener != null) {
			this.rListener.onResult(res);
		}
	}
	  
	public void registerResultListener(ResultListener rl) {
		this.rListener = rl;
	}
	
	@Override
	public void stop() {
	}
	
}

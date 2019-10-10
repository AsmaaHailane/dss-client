package io.nms.client.common;

import java.time.Instant;
import java.util.Arrays;
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
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;


public abstract class BaseClientVerticle extends AbstractVerticle {
	protected Logger LOG = LoggerFactory.getLogger(BaseClientVerticle.class);
	protected ResultListener rListener = null;
	
	protected abstract void requestAuthentication(String uname, String pass, Future<Void> prom);
	protected abstract void subscribeToResults(Receipt rct, Future<Void> prom);
	public abstract void discoverCapabilities(Future<List<Capability>> prom);
	public abstract void sendSpecification(Specification spec, Future<Receipt> prom);
	public abstract void sendInterrupt(Interrupt itr, Future<Receipt> prom); 
	public abstract void sendAdminReq(JsonObject req, Future<String> promise);
	
	private static final String ADDRESS = "client.*";
	
	EventBus eb = null;

	@Override
	public void start() {
		eb = vertx.eventBus();
		Router router = Router.router(vertx);
		
		SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
		BridgeOptions bridgeOptions = new BridgeOptions()
				.addInboundPermitted(new PermittedOptions().setAddressRegex(ADDRESS))
				.addOutboundPermitted(new PermittedOptions().setAddressRegex(ADDRESS));
		sockJSHandler.bridge(bridgeOptions);

		Set<String> allowedHeaders = new HashSet<>();
		allowedHeaders.add("x-requested-with");
		allowedHeaders.add("Access-Control-Allow-Origin");
		allowedHeaders.add("origin");
		allowedHeaders.add("Content-Type");
		allowedHeaders.add("accept");
		allowedHeaders.add("X-PINGARUNER");

		CorsHandler corsHandler = CorsHandler.create("http://10.11.200.213:8080").allowedHeaders(allowedHeaders)
				.allowCredentials(true);
		
		Arrays.asList(HttpMethod.values()).stream().forEach(method -> corsHandler.allowedMethod(method));
		router.route().handler(corsHandler);

		router.route("/eventbus/*").handler(sockJSHandler);
		
		vertx.createHttpServer().requestHandler(router::accept).listen(9000);

		eb.consumer("client", message -> {
			String action = message.headers().get("action");
			JsonObject payload = ((JsonObject) message.body());
			LOG.info("Eventbus message: "+action+ " | " +payload.encodePrettily());
			
			switch (action)
			{
			case "get.capabilities":
				getCapabilities(message);
				break;		

			case "send.specification":
				sendSpecification(message);
				break;
				
			case "get.results":
				getResults(message); 
				break;
				
			case "send.interrupt":
				sendInterrupt(message);
				break;
				
			case "get.clients":
				getClients(message);
				break;
				
			case "get.agents":
				getAgents(message); 
				break;
			
			default:
				message.reply("");
			}
		});
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
	
	
	protected void sendSpecification(Message<Object> message) {
		//LOG.info(((JsonObject)message.body()).encodePrettily());
		JsonObject payload = ((JsonObject) message.body());
		
		String strCap = payload.getString("capability");
		int duration = Integer.valueOf(payload.getString("duration"));
		int period = Integer.valueOf(payload.getString("period"));
		Capability cap = (Capability) io.nms.messages.Message.fromJsonString(strCap);
		Specification spec = new Specification(cap);
		
		long stop = Instant.now().plusSeconds(duration).toEpochMilli();
		String when = "now ... " + String.valueOf(stop) + " / " + period;
		spec.setWhen(when);
		LOG.info("Send Specification: "+io.nms.messages.Message.toJsonString(spec, true));
		Future<Receipt> fut = Future.future(rct -> sendSpecification(spec, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {
	        	message.reply(io.nms.messages.Message.toJsonString(res.result(), false));	        	
	        } else {
	        	LOG.error("Failed to get Receipt", res.cause());
	        	message.reply(new JsonObject().put("receipt", new JsonObject()));
	        	//message.reply("");
	        }
	    });
	}
	
	protected void getResults(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body());
		//String schema = payload.getString("schema");
		
    	JsonObject toStorageMsg = new JsonObject()
    		.put("action", "get_results_operation")
    		.put("payload", payload);
    	eb.send("dss.storage", toStorageMsg, reply -> {
    		if (reply.succeeded()) {
    			message.reply((String) reply.result().body());
    		} else {
    			LOG.error("Failed to get Results", reply.cause());
	        	message.reply(new JsonObject().put("results", new JsonObject()));
    		}
    	});
	}
	
	protected void sendInterrupt(Message<Object> message) {
		JsonObject payload = ((JsonObject) message.body()).getJsonObject("payload");
		Receipt receipt = (Receipt) io.nms.messages.Message.fromJsonString(payload.getString("receipt"));
		Interrupt interrupt = new Interrupt(receipt);
		String taskId = receipt.getContent("task.id");
		interrupt.setParameter("task.id", taskId);
		LOG.info("Send Interrupt: "+io.nms.messages.Message.toJsonString(interrupt, true));
		Future<Receipt> fut = Future.future(rct -> sendInterrupt(interrupt, rct));
		fut.setHandler(res -> {
	        if (res.succeeded()) {	    	 
	        	message.reply(io.nms.messages.Message.toJsonString(res.result(), false));	        	
	        } else {
	        	LOG.error("Failed to get Receipt", res.cause());
	        	message.reply(new JsonObject().put("receipt", new JsonObject()));
	        }
	    });
	}
	
	protected void processResult(io.nms.messages.Message res) {
		String resStr = io.nms.messages.Message.toJsonString(res, false);
		LOG.info("Got Result: "+resStr);
		
		// send to Console
		if (this.rListener != null) {
			this.rListener.onResult(res);
		}
		
		// send to GUI over eventbus
		eb.send("client.result", resStr, reply -> {
			if (reply.succeeded()) {
				LOG.info("Result sent to GUI.");
			}
		});
		
		// send to storage over eventbus
		JsonObject message = new JsonObject()
			.put("action", "add_result")
			.put("payload", new JsonObject(resStr));
			eb.send("dss.storage", message, reply -> {
				if (reply.succeeded()) {
					LOG.info("Result stored.");
				}
			});
	}
	  
	public void registerResultListener(ResultListener rl) {
		this.rListener = rl;
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		super.stop(stopFuture);
	}
	
}

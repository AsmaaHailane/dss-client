package io.nms.client.common;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import io.nms.storage.NmsEbMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;

public class RestAPIVerticle extends AbstractVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(RestAPIVerticle.class);
	private EventBus eb = null;
	private HttpServer hs = null;
	private int port = 9090;
	private static final String ADDRESS = "nms.*";
	private final String serviceName = "nms.rest";
	
	private int msgNbr = 0;
	
	@Override
	public void start(Future<Void> fut) {
		eb = vertx.eventBus();
		
		Router router = Router.router(vertx);
		
		// eventbus socket
		SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
		BridgeOptions bridgeOptions = new BridgeOptions()
				.addInboundPermitted(new PermittedOptions().setAddressRegex(ADDRESS))
				.addOutboundPermitted(new PermittedOptions().setAddressRegex(ADDRESS));
		sockJSHandler.bridge(bridgeOptions);

		Set<String> allowedHeaders = new HashSet<>();
		allowedHeaders.add("x-requested-with");
		allowedHeaders.add("Access-Control-Allow-Origin");
		allowedHeaders.add("Origin");
		allowedHeaders.add("Content-Type");
		allowedHeaders.add("Accept");
		allowedHeaders.add("X-PINGARUNER");

		CorsHandler corsHandler = CorsHandler.create("http://localhost:8080").allowedHeaders(allowedHeaders)
				.allowCredentials(true);
		
		Arrays.asList(HttpMethod.values()).stream().forEach(method -> corsHandler.allowedMethod(method));
		router.route().handler(corsHandler);
		
		// handle eventubs messages
		router.route("/eventbus/*").handler(sockJSHandler);
		
		// handle REST requests
		router.route("/nms*").handler(BodyHandler.create());
		router.post("/nms").handler(this::processRequest);
		
		hs = vertx
			.createHttpServer()
			.requestHandler(router::accept)
			.listen(port, res -> {
				if (res.failed()) {
					fut.fail(res.cause());
				} else {
					LOG.info("REST API service listening on port: " + port);
					initEbServiceInfo(fut);
				}
			});
	}
	
	private void initEbServiceInfo(Future<Void> fut) {		
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
			default:
				LOG.error("unknown action");
				replyUnknownAction(nmsEbMsg);
			}
			msgNbr++;
		});
		fut.complete();
	}
	
	private void getServiceInfo(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("action", message.getAction());		
		JsonObject content = new JsonObject()
			.put("name", "")
	        .put("role", "")
			.put("status", "running")
			.put("messages", msgNbr);
		response.put("content", content);
		message.reply(response);	      
	}
	
	protected void replyUnknownAction(NmsEbMessage message) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		response.put("action", message.getAction());
		response.put("error", "unknown action");
		message.reply(response);	      
	}
	
	
	/*
	 * curl -H "Content-Type: application/json" -XPOST -d 
	 * '{
	 * 		"service" : "nms.topology", 
	 * 		"query" : {
	 * 			"action" : "get.serviceino", 
	 * 			"params" : {}
	 * 		}
	 * 	}' 
	 * http://10.11.200.123:9000/nms
	 * */
	private void processRequest(RoutingContext routingContext) {
		JsonObject response = new JsonObject();
		response.put("service", serviceName);
		if (eb == null) {
			response.put("error", "eventbus not defined");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;
		}
		JsonObject request = routingContext.getBodyAsJson();
		if (request ==  null) {
			response.put("error", "no request defined");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;	
		}
		if (!request.containsKey("service")) {
			response.put("error", "target service not defined");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;	
		}
		String service = request.getString("service");
		if (service.isEmpty()) {
			response.put("error", "target service empty");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;	
		}
		
		publishLogging("Received request for service "+service);
		
		if (!request.containsKey("query")) {
			response.put("error", "query field not defined");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;	
		}
		JsonObject query = request.getJsonObject("query");
		if (!query.containsKey("action") || !query.containsKey("params")) {
			response.put("error", "action or params missing in query field");
			routingContext.response()
		    	.putHeader("content-type", "application/json; charset=utf-8")
		    	.end(response.encode());
			return;	
		}		
		msgNbr++;
		eb.send(service, query, reply -> {			
			if (reply.succeeded()) {				
				routingContext.response()
			    	.putHeader("content-type", "application/json; charset=utf-8")
			    	.end(((JsonObject)reply.result().body()).encode());
			} else {
				response.put("error", "failed to reach service");
				routingContext.response()
			    	.putHeader("content-type", "application/json; charset=utf-8")
			    	.end(response.encode());
		    }
		});
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
	public void stop(Future<Void> stopFuture) throws Exception {
		hs.close();
		eb.close(stopFuture);
	}
}

package io.nms.client.cli;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
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

public class Console extends AbstractVerticle {
	
	private static final Logger LOG = LoggerFactory.getLogger(Console.class);
	private EventBus eb = null;
	private int port = 9090;
	private static final String ADDRESS = "nms.*";
	private final String serviceName = "nms_rest";
	
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
		allowedHeaders.add("origin");
		allowedHeaders.add("Content-Type");
		allowedHeaders.add("accept");
		allowedHeaders.add("X-PINGARUNER");

		CorsHandler corsHandler = CorsHandler.create("http://10.11.200.213:8080").allowedHeaders(allowedHeaders)
				.allowCredentials(true);
		
		Arrays.asList(HttpMethod.values()).stream().forEach(method -> corsHandler.allowedMethod(method));
		router.route().handler(corsHandler);
		
		// handle eventubs messages
		router.route("/eventbus/*").handler(sockJSHandler);
		
		// handle REST requests
		router.route("/nms*").handler(BodyHandler.create());
		router.post("/nms").handler(this::processRequest);
		
		vertx
			.createHttpServer()
			.requestHandler(router::accept)
			.listen(port, res -> {
				if (res.failed()) {
					fut.fail(res.cause());
				} else {
					LOG.info("REST API service listening on port: " + port);
					fut.complete();
				}
			});
	}
	
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
			response.put("error", "target service field not defined");
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
		eb.send(service, query, reply -> {
			if (reply.succeeded()) {
				//LOG.info(((JsonObject)reply.result().body()).encodePrettily());
				routingContext.response()
			    	.putHeader("content-type", "application/json; charset=utf-8")
			    	.end(((JsonObject)reply.result().body()).encode());
			} else {
		    	//LOG.error("get.serviceinfo failed.", reply.cause());
				response.put("error", "failed to reach service");
				routingContext.response()
			    	.putHeader("content-type", "application/json; charset=utf-8")
			    	.end(response.encode());
		    }
		});
	}
	
	
	/*private static Console instance = null;
	private List<Capability> capabilities = new ArrayList<Capability>();
	private Receipt currentRcpt = null;
	private AmqpClientVerticle verticle = null;
	
	private Console(AmqpClientVerticle verticle) {
		this.verticle = verticle;
		this.verticle.registerResultListener(this);
	}
  
	public static Console getInstance(AmqpClientVerticle verticle) { 
		if (instance == null) {
			instance = new Console(verticle);
		}
		return instance;
	}

	@Override
	public void run() {
		Scanner input = new Scanner(System.in);
	    String[] opt = { "" };  
	    while (!"quit".equals(opt[0])) {
	    	System.out.println(">");
	    	opt[0] = input.nextLine();
	    	
	    	if ("clients".equals(opt[0])) {
	        	System.out.println("Get clients");
	        	JsonObject req = new JsonObject();
	        	req.put("action", "get_all_clients");
	        	req.put("payload", new JsonObject());
	        	Future<String> fut = Future
	        			.future(promise -> verticle.sendAdminReq(req, promise));
				fut.setHandler(res -> {
			        if (res.succeeded()) {
			        	JsonObject r = new JsonObject(res.result());
			        	System.out.println(r.getJsonArray("payload").encodePrettily());
			        } else {
			        	System.out.println("Error: "+res.cause());
			        }
			        System.out.println(">");
			    });
	        }
	    	if ("agents".equals(opt[0])) {
	        	System.out.println("Get agents");
	        	JsonObject req = new JsonObject();
	        	req.put("action", "get_all_agents");
	        	req.put("payload", new JsonObject());
	        	Future<String> fut = Future
	        			.future(promise -> verticle.sendAdminReq(req, promise));
				fut.setHandler(res -> {
			        if (res.succeeded()) {
			        	JsonObject r = new JsonObject(res.result());
			        	System.out.println(r.getJsonArray("payload").encodePrettily());
			        } else {
			        	System.out.println("Error: "+res.cause());
			        }
			        System.out.println(">");
			    });
	        }
	    	if ("discoverall".equals(opt[0])) {
	        	System.out.println("Discover All Capabilities");
	        	JsonObject req = new JsonObject();
	        	req.put("action", "get_caps_all");
	        	req.put("payload", new JsonObject());
	        	Future<String> fut = Future
	        			.future(promise -> verticle.sendAdminReq(req, promise));
				fut.setHandler(res -> {
			        if (res.succeeded()) {
			        	JsonObject r = new JsonObject(res.result());
			        	System.out.println(r.getJsonArray("payload").encodePrettily());
			        } else {
			        	System.out.println("Error: "+res.cause());
			        }
			        System.out.println(">");
			    });
	        }
	        if ("discover".equals(opt[0])) {
	        	System.out.println("Discover Capabilities");
	        	Future<List<Capability>> fut = Future
	        			.future(promise -> verticle.discoverCapabilities(promise));
				fut.setHandler(res -> {
			        if (res.succeeded()) {
			        	capabilities = res.result();
			        	printCapabilities();			      
			        }
			        System.out.println(">");
			    });
	        }
	        
	        // manage results
	        if ("res".equals(opt[0])) {
	        	System.out.println("Get all operations...");
	        	System.out.println(">");
	        }
	        
	        if ("spec".equals(opt[0])) {
	        	if (capabilities.isEmpty()) {
	        		System.out.println("No Capabilities.");
	        		System.out.println("Type 'discover' to retrieve Capabilities.");
	        	} else {
	        		System.out.println("Enter Capability No.");
	        		int c = Integer.parseInt(input.nextLine());
	        		if ((c >= 0) && (c < capabilities.size())) {
	        			createAndSendSpecification(capabilities.get(c)); 
	        		} else {
	        			System.out.println("Unknown Capability No.");
	        		}
	        	}
	        }
	        if (opt[0].isEmpty() && (currentRcpt != null)) {
	        	System.out.println("Interrupt Specification...");
	    		Interrupt interrupt = new Interrupt(currentRcpt);
	    		//String taskId = currentRcpt.getContent("task.id");
	    		//interrupt.setParameter("task.id", taskId);
	    		interrupt.setTimestampNow();
	    		Future<Receipt> fut = Future.future(rct -> verticle.sendInterrupt(interrupt, rct));
	    		fut.setHandler(res -> {
	    	        if (res.succeeded()) {	    	 
	    	        	printReceipt(res.result());	        	
	    	        }
	    	        currentRcpt = null;
	    	        System.out.println(">");
	    	    });
	        }
	    }
	    System.out.println("Goodbye.");
	    input.close();
	    System.exit(0);
	}
	
	private void printCapabilities() {
		for (int i = 0; i < capabilities.size(); i++) {
			System.out.print("["+i+"] ");
		    System.out.println(Message.toJsonString(capabilities.get(i), true));
		    System.out.println();
		}
	}
	
	private void createAndSendSpecification(Capability cap) {
		System.out.println("Create Specification for "+cap.getName());
		Scanner input = new Scanner(System.in);
		Specification spec = new Specification(cap);
		
		int pastS = 0;
		System.out.print("Start after (seconds): ");
		pastS = input.nextInt();
		input.nextLine();
		
		int futureS = 0;
		System.out.print("Stop after (seconds): ");
		futureS = input.nextInt();
		input.nextLine();
		
		int period = 0;
		System.out.print("Period (milliseconds): ");
		period = input.nextInt();
		input.nextLine();
		
		for (String key : cap.getParameters().keySet()) {
			System.out.print(key+" = ");
			String value = input.nextLine();
			spec.setParameter(key, value);
			System.out.println();
		}
		
		long start = Instant.now().plusSeconds(pastS).toEpochMilli();
		long stop = Instant.now().plusSeconds(futureS).toEpochMilli();
		
		String startS = "now";
		if (pastS != 0) {
			startS = String.valueOf(start);
		}
		String when = startS + " ... " + String.valueOf(stop) + " / " + period;
		
		spec.setWhen(when);
		
		System.out.println("Specification created:\n"+Message.toJsonString(spec, true));
		System.out.println("Send Specification y/n?");
		System.out.println("Type Enter to stop the Specification.");
		String answer = input.nextLine();
		if ("y".equals(answer)) {
			spec.setTimestampNow();
			Future<Receipt> fut = Future.future(rct -> verticle.sendSpecification(spec, rct));
			fut.setHandler(res -> {
		        if (res.succeeded()) {
		        	currentRcpt = res.result();
		        	printReceipt(currentRcpt);	        	
		        }
		    });
		}
	}
	
	private void printReceipt(Receipt rct) {
		System.out.println("Receipt: ");
		System.out.println(Message.toJsonString(rct, true));
	}

	@Override
	public void onResult(Message res) {
		System.out.println("Result: ");
		System.out.println(Message.toJsonString(res, true));
	}*/

}

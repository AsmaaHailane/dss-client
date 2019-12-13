package io.nms.client.cli;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.TimerTask;

import io.nms.client.common.AmqpClientVerticle;
import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Message;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public class Console extends TimerTask implements ResultListener {
	private static Console instance = null;
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
	}

}

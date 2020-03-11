package io.nms.client.main;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nms.client.common.BaseClientVerticle;
import io.nms.client.common.DataServiceVerticle;
import io.nms.client.common.RestAPIVerticle;
import io.nms.client.common.RoutingServiceVerticle;
import io.nms.client.common.TopologyServiceVerticle;
import io.nms.storage.StorageVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main { 
	private static Logger LOG = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) {
		if (args.length < 1) {
			LOG.error("No configuration file found.");
			System.exit(1); 
		}
		String configFile = args[0];
		JSONObject configuration = new JSONObject();
		try {
			LOG.info("Reading configuration file.");
			configuration = (JSONObject) new JSONParser().parse(new FileReader(configFile));
		} catch (IOException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (ParseException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
		
		LOG.info("Deploying services...");
		final JsonObject vertConfig = new JsonObject(configuration.toJSONString());
		
		List<Future> futures = new ArrayList<>();
		List<BaseClientVerticle> verticles = new ArrayList<>();
		
		Vertx vertx = Vertx.vertx();
		
		// deploy Data Streaming Service
		Future<Void> deployFuture1 = Future.future();
		futures.add(deployFuture1);
		BaseClientVerticle vDataService = new DataServiceVerticle();
		vertx.deployVerticle(vDataService, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.succeeded()) {
					LOG.info("Data Service deployed.");
					verticles.add(vDataService);
					deployFuture1.complete();
				} else {
					LOG.error("Failed to deploy DataServiceVerticle", res.cause());
					deployFuture1.fail(res.cause());
				}
			});
		
		// deploy topology service
		Future<Void> deployFuture2 = Future.future();
		futures.add(deployFuture2);
		BaseClientVerticle vTopologyService = new TopologyServiceVerticle();
		vertx.deployVerticle(vTopologyService, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.succeeded()) {
					LOG.info("Topology Service deployed.");
					verticles.add(vTopologyService);
					deployFuture2.complete();
				} else {
					LOG.error("Failed to deploy TopologyServiceVerticle", res.cause());
					deployFuture2.fail(res.cause());
				}
			});
		
		// deploy topology service
		Future<Void> deployFuture5 = Future.future();
		futures.add(deployFuture5);
		BaseClientVerticle vRoutingService = new RoutingServiceVerticle();
		vertx.deployVerticle(vRoutingService, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.succeeded()) {
					LOG.info("Routing Service deployed.");
					verticles.add(vRoutingService);
					deployFuture5.complete();
				} else {
					LOG.error("Failed to deploy RoutingServiceVerticle", res.cause());
					deployFuture5.fail(res.cause());
				}
			});
		
		// deploy storage service
		Future<Void> deployFuture3 = Future.future();
		futures.add(deployFuture3);
		StorageVerticle vStorageService = new StorageVerticle();
		vertx.deployVerticle(vStorageService, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.succeeded()) {
					LOG.info("Storage Service deployed.");
					deployFuture3.complete();
				} else {
					LOG.error("Failed to deploy StorageVerticle", res.cause());
					deployFuture3.fail(res.cause());
				}
			});
		
		// deploy REST service
		Future<Void> deployFuture4 = Future.future();
		futures.add(deployFuture4);
		RestAPIVerticle vConsole = new RestAPIVerticle();
		vertx.deployVerticle(vConsole, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.succeeded()) {
					LOG.info("REST Service deployed.");
					deployFuture4.complete();
				} else {
					LOG.error("Failed to deploy REST service", res.cause());
					deployFuture4.fail(res.cause());
				}
			});
		
		CompositeFuture.all(futures)
			.setHandler(ar -> {
				if (ar.failed()) {
					LOG.error("Error on starting NMS", ar.cause().getMessage());
					System.exit(1);
				} else {
					LOG.info(futures.size() + " Service(s) deployed.");
					LOG.info("NMS running...");
				}
			});
		
		Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
            	List<Future> futures = new ArrayList<>();
            	Future<Void> undeployFuture = Future.future();
    			
            	// stop common services
            	for (int i = 0; i < verticles.size(); i++) {
        			undeployFuture = Future.future();
        			futures.add(undeployFuture);
        			try {
        				verticles.get(i).stop(undeployFuture);
        			} catch (Exception e) {	
        				undeployFuture.fail(e.getMessage());
        			}
        		}
            	
            	// stop storage
            	undeployFuture = Future.future();
    			futures.add(undeployFuture);
    			try {
    				vStorageService.stop(undeployFuture);
    			} catch (Exception e) {	
    				undeployFuture.fail(e.getMessage());
    			}
    			
    			// stop REST
            	undeployFuture = Future.future();
    			futures.add(undeployFuture);
    			try {
    				vConsole.stop(undeployFuture);
    			} catch (Exception e) {	
    				undeployFuture.fail(e.getMessage());
    			}
            	
            	CompositeFuture.all(futures)
				.setHandler(ar -> {
					if (ar.failed()) {
						LOG.error("NMS terminated with errors", ar.cause());
			        	System.exit(1);
					} else {
						LOG.info(futures.size() + " Service(s) undeployed.");
						LOG.info("NMS successfully terminated.");
						System.exit(0);
					}
				});        	
            }
        });
	}
}
  
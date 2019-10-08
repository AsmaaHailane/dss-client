package io.nms.client.main;

import java.io.FileReader;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nms.client.cli.Console;
import io.nms.client.common.AmqpClientVerticle;
import io.nms.storage.StorageVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main {
	private static Logger LOG = LoggerFactory.getLogger(Main.class);
	private static Console cli; 
	
	public static void main(String[] args) {
		if (args.length < 1) {
			LOG.error("Configuration file required.");
			System.exit(1);
		}
		String configFile = args[0];
		JSONObject configuration = new JSONObject();
		try {
			LOG.info("Reading configuration file...");
			configuration = (JSONObject) new JSONParser().parse(new FileReader(configFile));
		} catch (IOException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		} catch (ParseException e) {
			LOG.error(e.getMessage());
			System.exit(1);
		}
		
		LOG.info("Deploying comunication module.");
		final Future<Void> commFut = Future.future();
		final JsonObject vertConfig = new JsonObject(configuration.toJSONString());
		Vertx vertx = Vertx.vertx();
		final AmqpClientVerticle vClient = new AmqpClientVerticle();
		
		vertx.deployVerticle(vClient, new DeploymentOptions()
			.setWorker(true)
			.setConfig(vertConfig),
			res -> {
				if (res.failed()) {
					LOG.info("Failed to deploy communication module.");
					commFut.fail(res.cause());
				} else {
					commFut.complete();
				}
			});
		
		// storage verticle
		LOG.info("Deploying storage module.");
		String[] deployId = {""};
		final Future<Void> storagefut = Future.future();
		vertx.deployVerticle(StorageVerticle.class.getName(), res -> {
			if (res.failed()) {
				LOG.error("Failed to deploy storage module");
				storagefut.fail(res.cause());
			} else {
				deployId[0] = res.result();
				storagefut.complete();
			}
		});
		
		CompositeFuture.all(commFut,storagefut)
		.setHandler(res -> {
			if (res.succeeded()) {
				LOG.info("Client ready.");
				cli = Console.getInstance(vClient);
				Timer timer = new Timer(true);
				timer.schedule((TimerTask) cli, 0);
			} else {
				LOG.error(res.cause().getMessage());
				System.exit(1);
			}
		});
	}
}
  
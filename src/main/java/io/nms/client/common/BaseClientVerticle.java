package io.nms.client.common;

import java.util.Arrays;
import java.util.HashMap;
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
	
	protected String clientName = "";
	protected String clientRole = "";
	
	protected HashMap<String, Receipt> activeSpecs = new HashMap<String, Receipt>();
	
	private static final String ADDRESS = "nms.*";
	EventBus eb = null;
	
	// implemented by AmqpVerticle
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
	public void start() {}
	
	protected void initEventBus(Future<Void> fut) {
		eb = vertx.eventBus();
		fut.complete();
		/*Router router = Router.router(vertx);
		
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
		
		setServiceApi();
		
		vertx.createHttpServer().requestHandler(router::accept).listen(9000, res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {		
				fut.complete();
			}
		});*/
	}
	
	
	  
	public void registerResultListener(ResultListener rl) {
		this.rListener = rl;
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		super.stop(stopFuture);
	}
	
}

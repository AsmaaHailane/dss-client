package io.nms.client.common;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nms.client.cli.ResultListener;
import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Message;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;


public abstract class BaseClientVerticle extends AbstractVerticle {
	protected Logger LOG = LoggerFactory.getLogger(BaseClientVerticle.class);
	protected ResultListener rListener = null;
	
	protected abstract void requestAuthentication(String uname, String pass, Future<Void> prom);
	protected abstract void subscribeToResults(Receipt rct, Future<Void> prom);
	public abstract void discoverCapabilities(Future<List<Capability>> prom);
	public abstract void sendSpecification(Specification spec, Future<Receipt> prom);
	public abstract void sendInterrupt(Interrupt itr, Future<Receipt> prom); 
	
	@Override
	public void start() {
	}
	
	protected void processResult(Message res) {
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

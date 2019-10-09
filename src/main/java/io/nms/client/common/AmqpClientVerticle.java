package io.nms.client.common;

import io.vertx.core.Future;

public class AmqpClientVerticle extends AmqpVerticle {
	
	public void start(Future<Void> fut) {
		super.start();
		
		String uname = config().getJsonObject("client").getString("username", "");
		String pass = config().getJsonObject("client").getString("password", "");
	
		String host = config().getJsonObject("amqp").getString("host", "");
		int port = config().getJsonObject("amqp").getInteger("port", 0);
		
		Future<Void> futConn = Future.future(promise -> createAmqpConnection(host, port, promise));
		Future<Void> futInit = futConn
			.compose(v -> {
				return Future.<Void>future(promise -> requestAuthentication(uname, pass, promise));
			});
		futInit.setHandler(res -> {
			if (res.failed()) {
				fut.fail(res.cause());
			} else {
				fut.complete();
			}
		});
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {
		LOG.info("Closing Client.");
		//LOG.info("Client logged out.");
		super.stop(stopFuture);
	} 
}

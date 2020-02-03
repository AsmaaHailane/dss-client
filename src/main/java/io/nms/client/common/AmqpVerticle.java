package io.nms.client.common;

import java.util.List;

import io.nms.messages.Capability;
import io.nms.messages.Interrupt;
import io.nms.messages.Message;
import io.nms.messages.Receipt;
import io.nms.messages.Specification;
import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpConnection;
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpReceiver;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public abstract class AmqpVerticle extends BaseClientVerticle {
	private AmqpConnection connection = null;
	
	/* Create AMQP connection to the broker */
	protected void createAmqpConnection(String host, int port, Future<Void> promise) {
		boolean isAmqp = !host.isEmpty() && (port > 0);
		if (isAmqp) {
			//LOG.info("Connecting to messaging platform...");
			AmqpClientOptions options = new AmqpClientOptions()
				.setHost(host)
				.setPort(port);
			AmqpClient client = AmqpClient.create(options);
			client.connect(ar -> {
				if (ar.failed()) {
					LOG.error("Unable to connect to the messaging platform", ar.cause());
					promise.fail(ar.cause());
				} else {
					LOG.info("Connected to the messaging platform.");
					connection = ar.result();
					promise.complete();
				}
			});
		} else {
			promise.fail("Wrong AMQP parameters");
		}
	}
	
	/* Client is sender. req-rep for authentication */	
	protected void requestAuthentication(String username, String password, Future<Void> promise) {
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					JsonObject repJson = new JsonObject(msg.bodyAsString());
					clientName = repJson.getString("name","");
					clientRole = repJson.getString("role","");
					if (clientName.isEmpty()) {
						promise.fail("Authentication failed.");
					} else {
						LOG.info("Client authenticated: "+repJson.encodePrettily());
						promise.complete();
					}
				});
				connection.createSender("/client/authentication", sender -> {
					if (sender.succeeded()) {
						JsonObject req = new JsonObject();
						req.put("username", username);
						req.put("password", password);
						sender.result().send(AmqpMessage.create()
						  .replyTo(replyToAddress)
						  .id("2")
						  .withBody(req.encode())
						  .build());
						LOG.info("Authenticating...");										
					} else {
						promise.fail(sender.cause());
					}
				});
			} else {
				promise.fail(replyReceiver.cause());
			}
		});
	}
	
	/* Client is sender. req-rep to retrieve Caps. */
	protected void discoverCapabilities(Future<List<Capability>> promise) {
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					//JsonObject ebRep = new JsonObject(msg.bodyAsString());
					//if (ebRep.containsKey("error")) {
					//	promise.fail(ebRep.getString("error"));
					//} else {
						promise.complete(Message.toListfromString(msg.bodyAsString()));
					//}
				});
				connection.createSender("/client/capabilities", sender -> {
					if (sender.succeeded()) {
						JsonObject req = new JsonObject();
						req.put("client_role", clientRole);
						sender.result().send(AmqpMessage.create()
					      .replyTo(replyToAddress)
					      .id("100")
					      .withBody(req.encode())
					      .build());
					}
				});
			}
		});
	}
	
	/* Client is sender. req-rep to issue Spec. */
	protected void sendSpecification(Specification spec, Future<Receipt> promise) { 
		spec.setToken(clientName);
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					Receipt rct = (Receipt) Message.fromJsonString(msg.bodyAsString());
					if (rct.getErrors().isEmpty()) {
						activeSpecs.put(rct.getSchema(), rct);				
						Future<Void> fut = Future.future(p -> subscribeToResults(rct, p));
						fut.setHandler(res -> {
					        if (res.failed()) {
					        	promise.fail(res.cause());
					        } else {				
					        	promise.complete(rct);					         
					        }
						});
					} else {
						promise.complete(rct);
					}
				});
				connection.createSender(spec.getEndpoint()+"/specifications", sender -> {
					if (sender.succeeded()) {
						sender.result().send(AmqpMessage.create()
						  .replyTo(replyToAddress)
						  .id("10")
						  .withBody(Message.toJsonString(spec, false)).build());
					}
				});
			} // else: cannot send spec...
		});
	}
	
	/* Client is subscriber. pub-sub for Client to get Results. */
	protected void subscribeToResults(Receipt rct, Future<Void> promise) {
		connection.createReceiver(rct.getEndpoint()+"/results/"+rct.getClientRole(),
			done -> {
				if (done.failed()) {
					LOG.error("Unable to subscribe to results", done.cause());
					promise.fail(done.cause());
				} else {
					AmqpReceiver receiver = done.result();
				    receiver
				    	.exceptionHandler(t -> {})
				        .handler(msg -> {
				        	processResult(Message.fromJsonString(msg.bodyAsString()));				  
				        });
				    LOG.info("Subscribed to results.");
				    promise.complete();
				}
			});
	}

	/* Client is sender. req-rep to retrieve Caps. */
	protected void sendInterrupt(Interrupt itr, Future<Receipt> promise) {
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					promise.complete((Receipt) Message.fromJsonString(msg.bodyAsString()));
				});
				connection.createSender(itr.getEndpoint()+"/specifications", sender -> {
					if (sender.succeeded()) {
						sender.result().send(AmqpMessage.create()
					      .replyTo(replyToAddress)
					      .id("100")
					      .withBody(Message.toJsonString(itr, false)).build());
					}
				});
			}
		});
	}
	
	/* Client is sender. req-rep to send Admin requests. 
	 * json request is set by caller; console, GUI.
	 * */
	protected void sendAdminReq(JsonObject req, Future<String> promise) { 
		//LOG.info("sending admin req: "+req.encodePrettily());
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					promise.complete(msg.bodyAsString());
				});
				connection.createSender("/admin", sender -> {
					if (sender.succeeded()) {
						sender.result().send(AmqpMessage.create()
					      .replyTo(replyToAddress)
					      .id("101")
					      .withBody(req.encode())
					      .build());
					} else {
						promise.fail(sender.cause());
					}
				});
			} else {
				promise.fail(replyReceiver.cause());
			}
		});
	}
	
	@Override
	public void stop(Future stopFuture) throws Exception {	
		Future<Void> futStop = Future.future(promise -> {
			try {
				super.stop(promise);
			} catch (Exception e) {
				LOG.error("Error on stopping", e.getMessage());
				promise.fail(e.getMessage());
			}
		});
		futStop.setHandler(res -> {
			LOG.info("AMQP connection closed.");
			connection.close(stopFuture);
		});
	}

}

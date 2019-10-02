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
	protected String clientName = "";
	private AmqpConnection connection = null;
	
	/* Create AMQP connection to the broker */
	protected void createAmqpConnection(String host, int port, Future<Void> promise) {
		boolean isAmqp = !host.isEmpty() && (port > 0);
		if (isAmqp) {
			LOG.info("Connecting to messaging platform...");
			AmqpClientOptions options = new AmqpClientOptions()
				.setHost(host)
				.setPort(port);
			AmqpClient client = AmqpClient.create(options);
			client.connect(ar -> {
				if (ar.failed()) {
					LOG.info("Unable to connect to the broker");
					promise.fail(ar.cause());
				} else {
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
		/* TODO: include username + password for authentication */
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					clientName = msg.bodyAsString();
					if (clientName.isEmpty()) {			
						promise.fail("Authentication failed.");
					} else {
						LOG.info("Got name: "+clientName);
						promise.complete();
					}			
				});
				connection.createSender("/authentication", sender -> {
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
	public void discoverCapabilities(Future<List<Capability>> promise) {
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					promise.complete(Message.toListfromString(msg.bodyAsString()));
				});
				connection.createSender("/dss/capabilities", sender -> {
					if (sender.succeeded()) {
						sender.result().send(AmqpMessage.create()
					      .replyTo(replyToAddress)
					      .id("100")
					      .withBody("role...").build());
					}
				});
			}
		});
	}
	
	/* Client is sender. req-rep to issue Spec. */
	public void sendSpecification(Specification spec, Future<Receipt> promise) {
		spec.setToken(clientName);
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					Receipt rct = (Receipt) Message.fromJsonString(msg.bodyAsString());
					Future<Void> fut = Future.future(p -> subscribeToResults(rct, p));
					fut.setHandler(res -> {
				        if (res.failed()) {
				        	promise.fail(res.cause());
				        } else {				
				        	promise.complete(rct);					         
				        }
					});
				});
				/* topic name found in Capability...*/
				connection.createSender(spec.getEndpoint()+"/specifications", sender -> {
					if (sender.succeeded()) {
						sender.result().send(AmqpMessage.create()
						  .replyTo(replyToAddress)
						  .id("10")
						  .withBody(Message.toJsonString(spec, false)).build());
					}
				});
			}
		});
	}
	
	/* Client is subscriber. pub-sub for Client to get Results. */
	protected void subscribeToResults(Receipt rct, Future<Void> promise) {
		/* topic name from receipt... */
		/* topic name: <agentName>/results/<role> */
		connection.createReceiver(rct.getEndpoint()+"/results",
			done -> {
				if (done.failed()) {
					System.out.println("Unable to create receiver");
					promise.fail(done.cause());
				} else {
					AmqpReceiver receiver = done.result();
				    receiver
				    	.exceptionHandler(t -> {})
				        .handler(msg -> {
				        	processResult(Message.fromJsonString(msg.bodyAsString()));				  
				        });
				    LOG.info("Agent Capabilities service initialized.");
				    promise.complete();
				}
			});
	}
	
	/* Client is sender. req-rep to retrieve Caps. */
	public void sendInterrupt(Interrupt itr, Future<Receipt> promise) {
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
	
	/* Client is sender. req-rep to retrieve Caps. */
	public void discoverUsers(Future<JsonArray> promise, String type) {
		connection.createDynamicReceiver(replyReceiver -> {
			if (replyReceiver.succeeded()) {
				String replyToAddress = replyReceiver.result().address();
				replyReceiver.result().handler(msg -> {
					promise.complete(msg.bodyAsJsonArray());
				});
				connection.createSender("/users", sender -> {
					if (sender.succeeded()) {
						JsonObject req = new JsonObject();
						req.put("type", type);
						sender.result().send(AmqpMessage.create()
					      .replyTo(replyToAddress)
					      .id("101")
					      .withBody(req.encode())
					      .build());
					}
				});
			}
		});
	}
	
	@Override
	public void stop() {
		super.stop();
	}

}

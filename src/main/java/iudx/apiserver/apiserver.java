/*Copyright 2018, Robert Bosch Centre for Cyber Physical Systems, Indian Institute of Science

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package iudx.apiserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;

/**
 * <h1>IUDX API Server</h1> An Open Source implementation of India Urban Data
 * Exchange (IUDX) platform APIs using Vert.x, an event driven and non-blocking
 * high performance reactive framework, for enabling seamless data exchange in
 * Smart Cities.
 * 
 * @author Swaminathan Vasanth Rajaraman <swaminathanvasanth.r@gmail.com>
 * @version 1.0.0
 */

public class apiserver extends AbstractVerticle implements Handler<HttpServerRequest>, Runnable {

	public static Vertx vertx;
	private HttpServer server;
	
	private HttpServerResponse resp;
	private String requested_id;
	private String requested_apikey;
	private String to;
	private String subject;
	private String message_type;
	private String connection_pool_id;	
	private JsonObject message;
	
	Map<String, RabbitMQClient> rabbitpool = new ConcurrentHashMap<String, RabbitMQClient>();
	Future<RabbitMQClient> broker_client;
	Future<RabbitMQClient> create_broker_client;
	RabbitMQOptions broker_config;
	RabbitMQClient client;
	
	public static String broker_url;
	public static String broker_username;
	public static String broker_password;
	public static int broker_port;
	public static String broker_vhost;
	
	// IUDX Base Path
	private static final String PATH_BASE = "/api/";
	
	// IUDX API version
	private static final String PATH_VERSION_1_0_0 = "1.0.0";
	
	// IUDX APIs
	private static final String PATH_PUBLISH = "/publish";
	
	// IUDX APIs ver. 1.0.0
	private static final String PATH_PUBLISH_version_1_0_0 = PATH_BASE+PATH_VERSION_1_0_0+PATH_PUBLISH;
	
	/**
	 * This method is used to setup and start the Vert.x server. It uses the
	 * available processors (n) to create (n*2) workers and also gets the available
	 * URLs from the URL class.
	 * 
	 * @param args Unused.
	 * @return Nothing.
	 * @exception Exception On setup or start error.
	 * @see Exception
	 */

	public static void main(String[] args) throws Exception {
		int procs = Runtime.getRuntime().availableProcessors();
		vertx = Vertx.vertx();
		vertx.exceptionHandler(err -> {
			err.printStackTrace();
		});

		vertx.deployVerticle(apiserver.class.getName(), new DeploymentOptions().setWorker(true).setInstances(procs * 2),
				event -> {
					if (event.succeeded()) {
						System.out.println("IUDX Vert.x API Server is started!");
					} else {
						System.out.println("Unable to start IUDX Vert.x API Server " + event.cause());
					}
				});
		broker_url = URLs.getBrokerUrl();
		broker_port = URLs.getBrokerPort();
		broker_vhost = URLs.getBrokerVhost();
		broker_username = URLs.getBrokerUsername();
		broker_password = URLs.getBrokerPassword();
	}
	
	/**
	 * This method is used to setup certificates for enabling HTTPs in Vert.x
	 * server. It uses the provided .jks (Java Key Store) certificate in the path
	 * and the password to enable SSL/TLS over HTTP in the desired port.
	 * 
	 * @param Nothing.
	 * @return Nothing.
	 * @exception Exception On start error.
	 * @see Exception
	 */
	
	@Override
	public void start() throws Exception {
		int port = 8443;
		server = vertx.createHttpServer(new HttpServerOptions().setSsl(true)
				.setKeyStoreOptions(new JksOptions().setPath("my-keystore.jks").setPassword("password")));
		server.requestHandler(apiserver.this).listen(port);
		HttpHeaders.createOptimized(
				java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
		vertx.setPeriodic(1000, handler -> {
			HttpHeaders.createOptimized(
					java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(java.time.ZonedDateTime.now()));
		});
	}
	
	@Override
	public void stop() {
		if (server != null)
			server.close();
	}

	
	@Override
	public void run() {
		
		
	}
	
	/**
	 * This method is used to handle the client requests and map it to the
	 * corresponding APIs using a switch case.
	 * 
	 * @param HttpServerRequest event This is the handle for the incoming request
	 *                          from client.
	 * @return Nothing.
	 */

	@Override
	public void handle(HttpServerRequest event) {
		switch (event.path()) {
		case PATH_PUBLISH_version_1_0_0:
			publish(event);
			break;
		}
	}
	
	/**
	 * This method is the implementation of Publish API, which handles the
	 * publication request by clients.
	 * 
	 * @param HttpServerRequest event This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp This sends the appropriate response for the
	 *         incoming request.
	 */

	private void publish(HttpServerRequest request) {
		
		request.bodyHandler(body -> {
			message = body.toJsonObject();
		});
		
		resp = request.response();
		requested_id = request.getHeader("id");
		requested_apikey = request.getHeader("apikey");
		to = request.getHeader("to");
		subject = request.getHeader("subject");
		message_type = request.getHeader("message-type");
		connection_pool_id = requested_id+requested_apikey;
		
		if (!(requested_id.equals(to))) {
			if (!(message_type.contains("command"))) {
				resp.setStatusCode(400).end();
			}
		} else if (!(message_type.contains("private") || message_type.contains("protected")
				|| message_type.contains("public") || message_type.contains("diagnostics"))) {
			resp.setStatusCode(400).end();
		}
		
		if (!rabbitpool.containsKey(connection_pool_id)) {
			broker_client = getRabbitMQClient(connection_pool_id, requested_id, requested_apikey);
			broker_client.setHandler(broker_client_start_handler -> {
				if (broker_client_start_handler.succeeded()) {
					rabbitpool.get(connection_pool_id).basicPublish(requested_id+"."+message_type,
							subject, message, null);
					resp.setStatusCode(200).end();
				}
			});
		} else {
			rabbitpool.get(connection_pool_id).basicPublish(requested_id+"."+message_type, subject,
					message, null);
			resp.setStatusCode(200).end();
		}
	}

	/**
	 * This method is used to create a connection pool of RabbitMQ clients.
	 * 
	 * @param String connection_pool_id This is the key for the ConcurrentHashMap.
	 *               The id is used to map the created connection to a
	 *               RabbitMQClient.
	 * @param String username This is the username to be used for the request to
	 *               RabbitMQ.
	 * @param String password This is the the password to be used for the request to
	 *               RabbitMQ
	 * @return Future<RabbitMQClient> create_broker_client This returns a Future
	 *         which represents the result of an asynchronous task.
	 */

	public Future<RabbitMQClient> getRabbitMQClient(String connection_pool_id, String username, String password) {

		create_broker_client = Future.future();
		broker_config = new RabbitMQOptions();
		broker_config.setHost(broker_url);
		broker_config.setPort(broker_port);
		broker_config.setVirtualHost(broker_vhost);
		broker_config.setUser(username);
		broker_config.setPassword(password);
		broker_config.setConnectionTimeout(6000);
		broker_config.setRequestedHeartbeat(60);
		broker_config.setHandshakeTimeout(6000);
		broker_config.setRequestedChannelMax(5);
		broker_config.setNetworkRecoveryInterval(500);

		client = RabbitMQClient.create(vertx, broker_config);
		client.start(start_handler -> {
			if (start_handler.succeeded()) {
				rabbitpool.put(connection_pool_id, client);
				create_broker_client.complete();

			}
		});

		  return create_broker_client;		
	}
	
}

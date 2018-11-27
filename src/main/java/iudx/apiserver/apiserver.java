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

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.RandomStringUtils;

import com.julienviet.pgclient.PgClient;
import com.julienviet.pgclient.PgIterator;
import com.julienviet.pgclient.PgPool;
import com.julienviet.pgclient.PgPoolOptions;
import com.julienviet.pgclient.Row;
import com.julienviet.pgclient.Tuple;

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
	
	/** This handles the HTTP response for all API requests */
	private HttpServerResponse resp;
	/** Handles the ID header in the HTTP Publish API request */
	
	private String requested_id;
	/** Handles the apikey header in the HTTP Publish API request */
	private String requested_apikey;
	/** Handles the to header in the HTTP Publish API request */
	private String to;
	/** Handles the subject header in the HTTP Publish API request */
	private String subject;
	/** Handles the message-type header in the HTTP Publish API request */
	private String message_type;

	/** Handles the owner / entity header in the HTTP Registration API request */
	private String requested_entity;
	/** Handles the updated entity name from the HTTP Registration API request */
	private String registration_entity_id;
	/** A boolean variable (FLAG) used for handling the ID availability */
	boolean entity_already_exists;
	
	/** Handles the Vert.x RabbitMQ client HashMap ID */
	private String connection_pool_id;	
	
	/**  Handles the message object for HTTP request and response */
	private JsonObject message;
	
	// Used in publish API
	/** Handles the Vert.x RabbitMQ client connections in a ConcurrentHashMap with a connection pool ID as key */
	Map<String, RabbitMQClient> rabbitpool = new ConcurrentHashMap<String, RabbitMQClient>();
	/**  A RabbitMQClient Future handler to notify the caller about the status of client connection */
	Future<RabbitMQClient> broker_client;
	/**  A RabbitMQClient Future handler to notify the caller about the status of client connection */
	Future<RabbitMQClient> create_broker_client;
	// Used in registration API
	/**  A RabbitMQClient Future handler to notify the caller about the status of client connection */
	Future<RabbitMQClient> init_connection;
	/**  A RabbitMQClient Future handler to notify the caller about the status of the requested exchange creation */
	Future<RabbitMQClient>  completed_exchange_entry_creation;
	/**  A RabbitMQClient Future handler to notify the caller about the status of the requested queue creation */
	Future<RabbitMQClient>  completed_queue_entry_creation;
	/**  A RabbitMQClient Future handler to notify the caller about the status of entity verification */
	Future<String> entity_verification;
	/**  A RabbitMQClient Future handler to notify the caller about the status of the requested exchange deletion */
	Future<RabbitMQClient>  completed_exchange_entry_deletion;
	/**  A RabbitMQClient Future handler to notify the caller about the status of the requested queue deletion */
	Future<RabbitMQClient>  completed_queue_entry_deletion;
	
	// Used in publish API
	/**  A RabbitMQClient configuration handler to modify connection parameters */
	RabbitMQOptions broker_config;
	/**  A RabbitMQ client to use the AMQP connection to interact with RabbitMQ */
	RabbitMQClient client;
	/**  Handles the URL at which RabbitMQ server is running */
	public static String broker_url;
	/**  Handles the username to be used to connect with RabbitMQ server */
	public static String broker_username;
	/**  Handles the password to be used to connect with RabbitMQ server */
	public static String broker_password;
	/**  Handles the port to be used to connect with RabbitMQ server */
	public static int broker_port;
	/**  Handles the virtual host to be used to connect with RabbitMQ server */
	public static String broker_vhost;
	
	// IUDX Base Path
	/**  Defines the API server base path */
	private static final String PATH_BASE = "/api/";
	
	// IUDX API version
	/**  Defines the version of API */
	private static final String PATH_VERSION_1_0_0 = "1.0.0";
	
	// IUDX APIs
	/**  Defines the Publish API endpoint */
	private static final String PATH_PUBLISH = "/publish";
	/**  Defines the registration API endpoint */
	private static final String PATH_REGISTER = "/register";
	/**  Defines the owner registration API endpoint */
	private static final String PATH_REGISTER_OWNER = "/register-owner";
	
	
	// IUDX APIs ver. 1.0.0
	/**  Defines the Publish API (1.0.0) endpoint */
	private static final String PATH_PUBLISH_version_1_0_0 = PATH_BASE+PATH_VERSION_1_0_0+PATH_PUBLISH;
	/**  Defines the Registration API (1.0.0) endpoint */
	private static final String PATH_REGISTRATION_version_1_0_0 = PATH_BASE+PATH_VERSION_1_0_0+PATH_REGISTER;
	/**  Defines the Owner Registration API (1.0.0) endpoint */
	private static final String PATH_OWNER_REGISTRATION_version_1_0_0 = PATH_BASE+PATH_VERSION_1_0_0+PATH_REGISTER_OWNER;
	
	// Used in registration API to connect with PostgresQL
	/**  A PostgresQL client pool to handle database connection */
	private PgPool database_client;
	/**  A PostgresQL handle for reading rows from the result set */
	private Tuple row;
	/**  A PostgresQL handle for iterating the result set of the executed Query */
	private PgIterator<Row> resultSet;
	/** A PostgresQL PgPoolOptions handle for configuring the connection parameters */
	private PgPoolOptions options;
	
	// Used in registration API for apikey generation
	/**  Characters to be used by APIKey generator while generating apikey */
	private static final String PASSWORDCHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-";
	/** Handles the generated apikey for the HTTP Registration API request */
	private String apikey;
	/** Handles the generated apikey hash for the HTTP Registration API request */
	private String apikey_hash;
	/** Handles the generated apikey hash (in bytes) for the HTTP Registration API request */
	private byte[] hash;
	/** A MessageDigest object used for creating the apikey hash */
	private MessageDigest digest;
	/** A boolean variable (FLAG) used for handling the state of the MessageDigest object */
	private boolean initiated_digest = false;

	
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
		/**  Defines the number of processors available in the server */
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
		/**  Defines the port at which the apiserver should run */
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
		
		options = new PgPoolOptions();
		options.setDatabase(URLs.psql_database_name);
		options.setHost(URLs.psql_database_url); 
		options.setPort(URLs.psql_database_port);
		options.setUsername(URLs.psql_database_username);
		options.setPassword(URLs.psql_database_password);
		options.setCachePreparedStatements(true);
		options.setMaxSize(5);
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
	 * @param HttpServerRequest event - This is the handle for the incoming request
	 *                          from client.
	 * @return Nothing.
	 */

	@Override
	public void handle(HttpServerRequest event) {
		switch (event.path()) {
		case PATH_OWNER_REGISTRATION_version_1_0_0:
			if(event.method().toString().equalsIgnoreCase("POST")) {
				register_owner(event);
				break;
			} else if(event.method().toString().equalsIgnoreCase("DELETE")) {
				de_register_owner(event);
				break;
			} else {
				resp = event.response();
				resp.setStatusCode(404).end();
				break;
			}
		case PATH_PUBLISH_version_1_0_0:
			publish(event);
			break;
		case PATH_REGISTRATION_version_1_0_0:
			if(event.method().toString().equalsIgnoreCase("POST")) {
				register(event);
				break;
			} else if(event.method().toString().equalsIgnoreCase("DELETE")) {
				de_register(event);
				break;
			} else {
				resp = event.response();
				resp.setStatusCode(404).end();
				break;
			}			
		}
	}
	
	
	/**
	 * This method is the implementation of owner Registration API, which handles
	 * new owner registration requests by IUDX admin.
	 * 
	 * @param HttpServerRequest req - This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp - This sends the appropriate response for the
	 *         incoming request.
	 */
	
	private void register_owner(HttpServerRequest req) {
		resp = req.response();
		database_client = PgClient.pool(vertx, options);
		requested_id = req.getHeader("id");
		if (!requested_id.equalsIgnoreCase("admin")) {
			resp.setStatusCode(401).end();
			return;
		} else {
		requested_apikey = req.getHeader("apikey");
		requested_entity = req.getHeader("owner");

		// Check if owner has a '/' in ID
		if (requested_entity.contains("/")) {
			resp.setStatusCode(401).end();
			return;
		} else {
			connection_pool_id = requested_id + requested_apikey;
			// Check if ID already exists
			entity_already_exists = false;
			entity_verification = verifyentity(requested_entity);
			entity_verification.setHandler(entity_verification_handler -> {
				if (entity_verification_handler.succeeded()) {
					if (entity_already_exists) {
						message = new JsonObject();
						message.put("conflict", "Owner ID already used");
						resp.setStatusCode(409).end(message.toString());
						return;
					} else {
						database_client.preparedQuery("SELECT * FROM users WHERE id = '" + requested_id + "'",
								database_response -> {
									if (database_response.succeeded()) {
										resultSet = database_response.result().iterator();
										if (!resultSet.hasNext()) {
											resp.setStatusCode(404).end();
											return;
										}
									}
									row = resultSet.next();
									generate_hash(requested_apikey);

									// Check the hash of admin with the hash in DB

									if (row.getString(1).equalsIgnoreCase(apikey_hash)
											&& row.getBoolean(4).toString().equalsIgnoreCase("false")) {
										generate_apikey();
										if (!rabbitpool.containsKey(connection_pool_id)) {
											init_connection = getRabbitMQClient(connection_pool_id, requested_id,
													requested_apikey);
											init_connection.setHandler(init_connection_handler -> {
												if (init_connection_handler.succeeded()) {
													client = rabbitpool.get(connection_pool_id);
													completed_exchange_entry_creation = createOwnerExchangeEntries();
													completed_queue_entry_creation = createOwnerQueueEntries();
													completed_exchange_entry_creation
															.setHandler(completed_exchange_entry_creation_handler -> {
																if (completed_exchange_entry_creation_handler
																		.succeeded()) {
																	completed_queue_entry_creation.setHandler(
																			completed_queue_entry_creation_handler -> {
																				if (completed_queue_entry_creation_handler
																						.succeeded()) {
																					Future<RabbitMQClient> completed_binding = createOwnerBindings();
																					completed_binding.setHandler(
																							completed_binding_handler -> {
																								if (completed_binding_handler
																										.succeeded()) {
																									database_client
																											.preparedQuery(
																													"INSERT INTO USERS VALUES ('"
																															+ requested_entity
																															+ "','"
																															+ apikey_hash
																															+ "',null,'"
																															+ hash
																															+ "','f','t')",
																													write_res -> {
																														if (write_res
																																.succeeded()) {
																															message = new JsonObject();
																															message.put(
																																	"id",
																																	requested_entity);
																															message.put(
																																	"apikey",
																																	apikey);
																															resp.setStatusCode(
																																	200)
																																	.end(message
																																			.toString());
																															return;
																														}
																													});
																								}
																							});
																				}
																			});
																}
															});
												}
											});
										} else {
											client = rabbitpool.get(connection_pool_id);
											completed_exchange_entry_creation = createOwnerExchangeEntries();
											completed_queue_entry_creation = createOwnerQueueEntries();
											completed_exchange_entry_creation
													.setHandler(completed_exchange_entry_creation_handler -> {
														if (completed_exchange_entry_creation_handler.succeeded()) {
															completed_queue_entry_creation.setHandler(
																	completed_queue_entry_creation_handler -> {
																		if (completed_queue_entry_creation_handler
																				.succeeded()) {
																			Future<RabbitMQClient> completed_binding = createOwnerBindings();
																			completed_binding.setHandler(
																					completed_binding_handler -> {
																						if (completed_binding_handler
																								.succeeded()) {
																							database_client
																									.preparedQuery(
																											"INSERT INTO USERS VALUES ('"
																													+ requested_entity
																													+ "',' "
																													+ apikey_hash
																													+ "',null,'"
																													+ hash
																													+ "','f','t')",
																											write_res -> {
																												if (write_res
																														.succeeded()) {
																													message.put(
																															"id",
																															requested_entity);
																													message.put(
																															"apikey",
																															apikey);
																													resp.setStatusCode(
																															200)
																															.end(message
																																	.toString());
																													return;
																												}
																											});
																						}
																					});
																		}
																	});
														}
													});
										}
									}
								});
					}
				} else {
					resp.setStatusCode(404).end();
					return;
				}
			});
		}
		}
	}
	
	
	/**
	 * This method is the implementation of owner De-Registration API, which handles
	 * owner de-registration requests by IUDX admin.
	 * 
	 * @param HttpServerRequest req - This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp - This sends the appropriate response for the
	 *         incoming request.
	 */
	
	private void de_register_owner(HttpServerRequest req) {

		resp = req.response();
		database_client = PgClient.pool(vertx, options);
		requested_id = req.getHeader("id");
		if (!requested_id.equalsIgnoreCase("admin")) {
			resp.setStatusCode(401).end();
			return;
		} else {
		requested_apikey = req.getHeader("apikey");
		requested_entity = req.getHeader("owner");

		// Check if owner has a '/' in ID
		if (requested_entity.contains("/")) {
			resp.setStatusCode(401).end();
			return;
		} else {
			connection_pool_id = requested_id + requested_apikey;
			// Check if ID already exists
			entity_already_exists = true;
			entity_verification = verifyentity(requested_entity);
			entity_verification.setHandler(entity_verification_handler -> {
				if (entity_verification_handler.succeeded()) {
					if (! entity_already_exists) {
						message = new JsonObject();
						message.put("failure", "Owner ID not found");
						resp.setStatusCode(401).end(message.toString());
						return;
					} else {
						database_client.preparedQuery("SELECT * FROM users WHERE id = '" + requested_id + "'",
								database_response -> {
									if (database_response.succeeded()) {
										resultSet = database_response.result().iterator();
										if (!resultSet.hasNext()) {
											resp.setStatusCode(404).end();
											return;
										}
									}
									row = resultSet.next();
									generate_hash(requested_apikey);

									// Check the hash of admin with the hash in DB

									if (row.getString(1).equalsIgnoreCase(apikey_hash)
											&& row.getBoolean(4).toString().equalsIgnoreCase("false")) {
										if (!rabbitpool.containsKey(connection_pool_id)) {
											init_connection = getRabbitMQClient(connection_pool_id, requested_id,
													requested_apikey);
											init_connection.setHandler(init_connection_handler -> {
												if (init_connection_handler.succeeded()) {
													client = rabbitpool.get(connection_pool_id);
													completed_exchange_entry_deletion = deleteOwnerExchangeEntries();
													completed_queue_entry_deletion = deleteOwnerQueueEntries();
													completed_exchange_entry_deletion
															.setHandler(completed_exchange_entry_deletion_handler -> {
																if (completed_exchange_entry_deletion_handler
																		.succeeded()) {
																	completed_queue_entry_deletion.setHandler(
																			completed_queue_entry_deletion_handler -> {
																				if (completed_queue_entry_deletion_handler
																						.succeeded()) {
																					database_client
																							.preparedQuery(
																									"DELETE FROM USERS WHERE ID = '"
																											+ requested_entity
																											+ "'",
																									delete_res -> {
																										if (delete_res
																												.succeeded()) {
																											resp.setStatusCode(
																													200)
																													.end();
																											return;
																										}
																									});
																				}
																			});
																}
															});
												}
											});
										} else {
											client = rabbitpool.get(connection_pool_id);
											completed_exchange_entry_deletion = deleteOwnerExchangeEntries();
											completed_queue_entry_deletion = deleteOwnerQueueEntries();
											completed_exchange_entry_deletion
													.setHandler(completed_exchange_entry_deletion_handler -> {
														if (completed_exchange_entry_deletion_handler
																.succeeded()) {
															completed_queue_entry_deletion.setHandler(
																	completed_queue_entry_deletion_handler -> {
																		if (completed_queue_entry_deletion_handler
																				.succeeded()) {
																			database_client
																					.preparedQuery(
																							"DELETE FROM USERS WHERE ID = '"
																									+ requested_entity
																									+ "'",
																							delete_res -> {
																								if (delete_res
																										.succeeded()) {
																									resp.setStatusCode(
																											200)
																											.end();
																									return;
																								}
																							});
																		}
																	});
														}
													});
										}
									}
								});
					}
				} else {
					resp.setStatusCode(404).end();
					return;
				}
			});
		}
		}
	}
	
	/**
	 * This method is the implementation of entity Registration API, which handles
	 * the new device or application registration requests by owners.
	 * 
	 * @param HttpServerRequest req - This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp - This sends the appropriate response for the
	 *         incoming request.
	 */

	private void register(HttpServerRequest req) {

		resp = req.response();
		database_client = PgClient.pool(vertx, options);
		requested_id = req.getHeader("id");

		// Check if ID is owner
		if (requested_id.contains("/")) {
			resp.setStatusCode(401).end();
			return;
		} else {
			requested_apikey = req.getHeader("apikey");
			requested_entity = req.getHeader("entity");
			connection_pool_id = requested_id + requested_apikey;
			registration_entity_id = requested_id + "/" + requested_entity;

			// Check if ID already exists
			entity_already_exists = false;
			entity_verification = verifyentity(registration_entity_id);

			entity_verification.setHandler(entity_verification_handler -> {

				if (entity_verification_handler.succeeded()) {
					if (entity_already_exists) {
						message = new JsonObject();
						message.put("conflict", "entity ID already used");
						resp.setStatusCode(409).end(message.toString());
						return;
					} else {
						database_client.preparedQuery("SELECT * FROM users WHERE id = '" + requested_id + "'",
								database_response -> {
									if (database_response.succeeded()) {
										resultSet = database_response.result().iterator();
										if (!resultSet.hasNext()) {
											resp.setStatusCode(404).end();
											return;
										}

										row = resultSet.next();
										generate_hash(requested_apikey);

										// Check the hash of owner with the hash in DB
										// Check if blocked is false

										if (row.getString(1).equalsIgnoreCase(apikey_hash)
												&& row.getBoolean(4).toString().equalsIgnoreCase("false")) {
											generate_apikey();
											if (!rabbitpool.containsKey(connection_pool_id)) {
												init_connection = getRabbitMQClient(connection_pool_id, requested_id,
														requested_apikey);
												init_connection.setHandler(init_connection_handler -> {
													if (init_connection_handler.succeeded()) {
														client = rabbitpool.get(connection_pool_id);
														completed_exchange_entry_creation = createExchangeEntries();
														completed_queue_entry_creation = createQueueEntries();
														completed_exchange_entry_creation.setHandler(
																completed_exchange_entry_creation_handler -> {
																	if (completed_exchange_entry_creation_handler
																			.succeeded()) {
																		completed_queue_entry_creation.setHandler(
																				completed_queue_entry_creation_handler -> {
																					if (completed_queue_entry_creation_handler
																							.succeeded()) {
																						Future<RabbitMQClient> completed_binding = createBindings();
																						completed_binding.setHandler(
																								completed_binding_handler -> {
																									if (completed_binding_handler
																											.succeeded()) {
																										database_client
																												.preparedQuery(
																														"INSERT INTO USERS VALUES ('"
																																+ registration_entity_id
																																+ "','"
																																+ apikey_hash
																																+ "',null,'"
																																+ hash
																																+ "','f','t')",
																														write_res -> {
																															if (write_res
																																	.succeeded()) {
																																message = new JsonObject();
																																message.put(
																																		"id",
																																		registration_entity_id);
																																message.put(
																																		"apikey",
																																		apikey);
																																resp.setStatusCode(
																																		200)
																																		.end(message
																																				.toString());
																																return;
																															}
																														});
																									}
																								});

																					}

																				});
																	}
																});
													}
												});

											} else {
												client = rabbitpool.get(connection_pool_id);
												completed_exchange_entry_creation = createExchangeEntries();
												completed_queue_entry_creation = createQueueEntries();

												completed_exchange_entry_creation
														.setHandler(completed_exchange_entry_creation_handler -> {
															if (completed_exchange_entry_creation_handler.succeeded()) {
																completed_queue_entry_creation.setHandler(
																		completed_queue_entry_creation_handler -> {
																			if (completed_queue_entry_creation_handler
																					.succeeded()) {
																				Future<RabbitMQClient> completed_binding = createBindings();
																				completed_binding.setHandler(
																						completed_binding_handler -> {
																							if (completed_binding_handler
																									.succeeded()) {
																								database_client
																										.preparedQuery(
																												"INSERT INTO USERS VALUES ('"
																														+ registration_entity_id
																														+ "',' "
																														+ apikey_hash
																														+ "',null,'"
																														+ hash
																														+ "','f','t')",
																												write_res -> {
																													if (write_res
																															.succeeded()) {
																														message.put(
																																"id",
																																registration_entity_id);
																														message.put(
																																"apikey",
																																apikey);
																														resp.setStatusCode(
																																200)
																																.end(message
																																		.toString());
																														return;
																													}
																												});
																							}
																						});
																			}
																		});
															}
														});
											}
										}
									} else {
										resp.setStatusCode(404).end();
										return;
									}
								});

					}

				}
			});
		}
	}
	
	private void de_register(HttpServerRequest req) {

		resp = req.response();
		database_client = PgClient.pool(vertx, options);
		requested_id = req.getHeader("id");

		// Check if ID is owner
		if (requested_id.contains("/")) {
			resp.setStatusCode(401).end();
			return;
		} else {
			requested_apikey = req.getHeader("apikey");
			requested_entity = req.getHeader("entity");
			// Check if entity header contains a '/'
			if (requested_entity.contains("/")) {
				resp.setStatusCode(401).end();
				return;
			} else {
			connection_pool_id = requested_id + requested_apikey;
			registration_entity_id = requested_id + "/" + requested_entity;

			// Check if ID already exists
			entity_already_exists = true;
			entity_verification = verifyentity(registration_entity_id);

			entity_verification.setHandler(entity_verification_handler -> {

				if (entity_verification_handler.succeeded()) {
					if (! entity_already_exists) {
						message = new JsonObject();
						message.put("failure", "Entity ID not found");
						resp.setStatusCode(401).end(message.toString());
						return;
					} else {
						database_client.preparedQuery("SELECT * FROM users WHERE id = '" + requested_id + "'",
								database_response -> {
									if (database_response.succeeded()) {
										resultSet = database_response.result().iterator();
										if (!resultSet.hasNext()) {
											resp.setStatusCode(404).end();
											return;
										}

										row = resultSet.next();
										generate_hash(requested_apikey);

										// Check the hash of owner with the hash in DB
										// Check if blocked is false

										if (row.getString(1).equalsIgnoreCase(apikey_hash)
												&& row.getBoolean(4).toString().equalsIgnoreCase("false")) {
											
											if (!rabbitpool.containsKey(connection_pool_id)) {
												init_connection = getRabbitMQClient(connection_pool_id, requested_id,
														requested_apikey);
												init_connection.setHandler(init_connection_handler -> {
													if (init_connection_handler.succeeded()) {
														client = rabbitpool.get(connection_pool_id);
														completed_exchange_entry_deletion = deleteExchangeEntries();
														completed_queue_entry_deletion = deleteQueueEntries();
														completed_exchange_entry_deletion.setHandler(
																completed_exchange_entry_deletion_handler -> {
																	if (completed_exchange_entry_deletion_handler
																			.succeeded()) {
																		completed_queue_entry_deletion.setHandler(
																				completed_queue_entry_deletion_handler -> {
																					if (completed_queue_entry_deletion_handler
																							.succeeded()) {
																						database_client
																								.preparedQuery(
																										"DELETE FROM USERS WHERE ID = '"
																												+ registration_entity_id
																												+ "'",
																										delete_res -> {
																											if (delete_res
																													.succeeded()) {
																												resp.setStatusCode(
																														200)
																														.end();
																												return;
																											}
																										});
																					}
																				});
																	}
																});
													}
												});

											} else {
												client = rabbitpool.get(connection_pool_id);
												completed_exchange_entry_deletion = deleteExchangeEntries();
												completed_queue_entry_deletion = deleteQueueEntries();
												completed_exchange_entry_deletion.setHandler(
														completed_exchange_entry_deletion_handler -> {
															if (completed_exchange_entry_deletion_handler
																	.succeeded()) {
																completed_queue_entry_deletion.setHandler(
																		completed_queue_entry_deletion_handler -> {
																			if (completed_queue_entry_deletion_handler
																					.succeeded()) {
																				database_client
																						.preparedQuery(
																								"DELETE FROM USERS WHERE ID = '"
																										+ registration_entity_id
																										+ "'",
																								delete_res -> {
																									if (delete_res
																											.succeeded()) {
																										resp.setStatusCode(
																												200)
																												.end();
																										return;
																									}
																								});
																			}
																		});
															}
														});
											}
										}
									} else {
										resp.setStatusCode(404).end();
										return;
									}
								});

					}

				}
			});
		}
	}
	}

	
	/**
	 * This method is used to verify if the requested registration entity is already
	 * registered.
	 * 
	 * @param String registration_entity_id - This is the handle for the incoming
	 *               request header (entity) from client.
	 * @return Future<String> verifyentity - This is a callable Future which notifies
	 *         on completion.
	 */
	
	private Future<String> verifyentity(String registration_entity_id) {
		Future<String> verifyentity = Future.future();
		database_client.preparedQuery("SELECT * FROM users WHERE id = '"+registration_entity_id+"'", database_response -> {
			if(database_response.succeeded()) {
				resultSet = database_response.result().iterator();
				if (!resultSet.hasNext()) {
					entity_already_exists = false;
				} else {
					row = resultSet.next();
					if (row.getString(0).equalsIgnoreCase(registration_entity_id) && row.getBoolean(4).toString().equalsIgnoreCase("false")) {
						entity_already_exists = true;
					}
				}
			}
			verifyentity.complete();
		});
		return verifyentity;		
	}

	/**
	 * This method is used to generate apikey using the allowed characters. Once
	 * apikey is created, a hash of it is also created. The entries are stored in
	 * the variables apikey, hash (as bytes) and apikey_hash (as String).
	 * 
	 * @param Nothing.
	 * @return void Nothing.
	 */

	private void generate_apikey() {
		apikey = RandomStringUtils.random(32, 0, PASSWORDCHARS.length(), true, true, PASSWORDCHARS.toCharArray());

		try {
			if(!initiated_digest) {
			digest = MessageDigest.getInstance("SHA-256");
			initiated_digest = true;}
			digest.update(apikey.getBytes("UTF-8"));
			hash = digest.digest();
			convert_byte_to_string(hash);
			
		} catch (Exception e) {
			System.out.println("UnsupportedEncodingException");
		}
	}

	/**
	 * This method is used to generate apikey hash from the received apikey. This is
	 * used for validating the user. The entry is stored in the variable hash (as
	 * bytes) and in apikey_hash (as String).
	 * 
	 * @param String key - The key which needs to be converted to a hash.
	 * @return byte[] hash - The hash which can now be compared in the database.
	 */

	private byte[] generate_hash(String key) {
		
		try {
			if(!initiated_digest) {
			digest = MessageDigest.getInstance("SHA-256");
			initiated_digest = true;}
			digest.update(key.getBytes("UTF-8"));
			hash = digest.digest();
			convert_byte_to_string(hash);
		} catch (Exception e) {
			System.out.println("UnsupportedEncodingException");
		}
		return hash;
	}


	/**
	 * This method is used to convert byte (apikey) to String (apikey_hash).
	 * 
	 * @param byte[] hash - The hash (in bytes) which needs to be converted to a
	 *        String.
	 * @return void Nothing.
	 */
	
	private void convert_byte_to_string(byte[] hash) {
		StringBuilder sb = new StringBuilder();
        for(int i=0; i< hash.length ;i++)
        {
            sb.append(Integer.toString((hash[i] & 0xff) + 0x100, 16).substring(1));
        }
        //Get complete hashed password in hex format
        apikey_hash = sb.toString();
 	}
	
	/**
	 * This method is used to create exchanges in RabbitMQ for Owners.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createOwnerExchangeEntries - This is a callable
	 *         Future which notifies the caller on completion of exchange creation.
	 */
	
	private Future<RabbitMQClient> createOwnerExchangeEntries() {
		Future<RabbitMQClient> createOwnerExchangeEntries = Future.future();
		client.exchangeDeclare(requested_entity + ".notification", "topic", true, false,
				notification_exchange_handler -> {
					if (notification_exchange_handler.succeeded()) {
						createOwnerExchangeEntries.complete();
					}
				});
		return createOwnerExchangeEntries;
	}

	/**
	 * This method is used to delete exchanges in RabbitMQ for Owners.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> deleteOwnerExchangeEntries - This is a callable
	 *         Future which notifies the caller on completion of exchange deletion.
	 */
	
	private Future<RabbitMQClient> deleteOwnerExchangeEntries() {
		Future<RabbitMQClient> deleteOwnerExchangeEntries = Future.future();
		client.exchangeDelete(requested_entity + ".notification", notification_exchange_handler -> {
			if (notification_exchange_handler.succeeded()) {
				deleteOwnerExchangeEntries.complete();
			}
		});
		return deleteOwnerExchangeEntries;
	}
	
	/**
	 * This method is used to create queues in RabbitMQ for Owners.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createOwnerQueueEntries - This is a callable Future
	 *         which notifies the caller on completion of queue creation.
	 */
	
	private Future<RabbitMQClient> createOwnerQueueEntries() {
		Future<RabbitMQClient> createOwnerQueueEntries = Future.future();
		client.queueDeclare(requested_entity + ".notification", true, false, false, notification_queue_handler -> {
			if (notification_queue_handler.succeeded()) {
				createOwnerQueueEntries.complete();

			}
		});
		return createOwnerQueueEntries;
	}

	/**
	 * This method is used to delete queues in RabbitMQ for Owners.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> deleteOwnerQueueEntries - This is a callable Future
	 *         which notifies the caller on completion of queue deletion.
	 */
	
	private Future<RabbitMQClient> deleteOwnerQueueEntries() {
		Future<RabbitMQClient> deleteOwnerQueueEntries = Future.future();
		client.queueDelete(requested_entity + ".notification", notification_queue_handler -> {
			if (notification_queue_handler.succeeded()) {
				deleteOwnerQueueEntries.complete();

			}
		});
		return deleteOwnerQueueEntries;
	}
	
	/**
	 * This method is used to create bindings in RabbitMQ for Owners.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createOwnerBindings - This is a callable Future which
	 *         notifies the caller on completion of bindings.
	 */
	
	private Future<RabbitMQClient> createOwnerBindings() {
		Future<RabbitMQClient> createOwnerBindings = Future.future();
		client.queueBind(requested_entity + ".notification", requested_entity + ".notification", "#",
				queue_bind_handler -> {
					if (queue_bind_handler.succeeded()) {
						client.queueBind("database", requested_entity + ".notification", "#",
								database_notification_bind_handler -> {
									if (database_notification_bind_handler.succeeded()) {
										createOwnerBindings.complete();
									}
								});
					}
				});
		return createOwnerBindings;
	}	
	
	/**
	 * This method is used to create exchanges in RabbitMQ for entities.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createExchangeEntries - This is a callable
	 *         Future which notifies the caller on completion of exchange creation.
	 */
	
	private Future<RabbitMQClient> createExchangeEntries() {
		Future<RabbitMQClient> createExchangeEntries = Future.future();
		client.exchangeDeclare(registration_entity_id + ".private", "topic", true, false, private_exchange_handler -> {
			if (private_exchange_handler.succeeded()) {
				client.exchangeDeclare(registration_entity_id + ".public", "topic", true, false, public_exchange_handler -> {
					if (public_exchange_handler.succeeded()) {
						client.exchangeDeclare(registration_entity_id + ".protected", "topic", true, false, protected_exchange_handler -> {
							if (protected_exchange_handler.succeeded()) {
								client.exchangeDeclare(registration_entity_id + ".diagnostics", "topic", true, false, diagnostics_exchange_handler -> {
									if (diagnostics_exchange_handler.succeeded()) {
										client.exchangeDeclare(registration_entity_id + ".notification", "topic", true, false, notification_exchange_handler -> {
											if (notification_exchange_handler.succeeded()) {
												client.exchangeDeclare(registration_entity_id + ".publish", "topic", true, false, publish_exchange_handler -> {
													if (publish_exchange_handler.succeeded()) {
														createExchangeEntries.complete();
																											}
																										});
																					}
																				});
																	}
																});
													}
												});
									}
								});
					}
				});
		return createExchangeEntries;
	}

	private Future<RabbitMQClient> deleteExchangeEntries() {
		Future<RabbitMQClient> deleteExchangeEntries = Future.future();
		client.exchangeDelete(registration_entity_id + ".private", private_exchange_handler -> {
			if (private_exchange_handler.succeeded()) {
				client.exchangeDelete(registration_entity_id + ".public", public_exchange_handler -> {
					if (public_exchange_handler.succeeded()) {
						client.exchangeDelete(registration_entity_id + ".protected", protected_exchange_handler -> {
							if (protected_exchange_handler.succeeded()) {
								client.exchangeDelete(registration_entity_id + ".diagnostics", diagnostics_exchange_handler -> {
									if (diagnostics_exchange_handler.succeeded()) {
										client.exchangeDelete(registration_entity_id + ".notification", notification_exchange_handler -> {
											if (notification_exchange_handler.succeeded()) {
												client.exchangeDelete(registration_entity_id + ".publish", publish_exchange_handler -> {
													if (publish_exchange_handler.succeeded()) {
														deleteExchangeEntries.complete();
																											}
																										});
																					}
																				});
																	}
																});
													}
												});
									}
								});
					}
				});
		return deleteExchangeEntries;
	}
	
	/**
	 * This method is used to create queues in RabbitMQ for entities.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createQueueEntries - This is a callable Future
	 *         which notifies the caller on completion of queue creation.
	 */
	
	private Future<RabbitMQClient> createQueueEntries() {
		Future<RabbitMQClient> createQueueEntries = Future.future();
		client.queueDeclare(registration_entity_id, true, false, false, queue_handler -> {
			if(queue_handler.succeeded()) {
				client.queueDeclare(registration_entity_id + ".private", true, false, false, private_queue_handler -> {
					if(private_queue_handler.succeeded()) {
						client.queueDeclare(registration_entity_id + ".priority", true, false, false, priority_queue_handler -> {
							if(priority_queue_handler.succeeded()) {
								client.queueDeclare(registration_entity_id + ".command", true, false, false, command_queue_handler -> {
									if(private_queue_handler.succeeded()) {
										client.queueDeclare(registration_entity_id + ".notification", true, false, false, notification_queue_handler -> {
											if(notification_queue_handler.succeeded()) {
												createQueueEntries.complete();
												
																			}
																		});
															}
														});
											}
										});
							}
						});
			}
		});
		return createQueueEntries;
	}

	private Future<RabbitMQClient> deleteQueueEntries() {
		Future<RabbitMQClient> deleteQueueEntries = Future.future();
		client.queueDelete(registration_entity_id, queue_handler -> {
			if(queue_handler.succeeded()) {
				client.queueDelete(registration_entity_id + ".private", private_queue_handler -> {
					if(private_queue_handler.succeeded()) {
						client.queueDelete(registration_entity_id + ".priority", priority_queue_handler -> {
							if(priority_queue_handler.succeeded()) {
								client.queueDelete(registration_entity_id + ".command", command_queue_handler -> {
									if(private_queue_handler.succeeded()) {
										client.queueDelete(registration_entity_id + ".notification", notification_queue_handler -> {
											if(notification_queue_handler.succeeded()) {
												deleteQueueEntries.complete();
												
																			}
																		});
															}
														});
											}
										});
							}
						});
			}
		});
		return deleteQueueEntries;
	}

	
	/**
	 * This method is used to create bindings in RabbitMQ for entities.
	 * 
	 * @param Nothing.
	 * @return Future<RabbitMQClient> createBindings - This is a callable Future which
	 *         notifies the caller on completion of bindings.
	 */

	private Future<RabbitMQClient> createBindings() {
		Future<RabbitMQClient> createBindings = Future.future();
		client.queueBind(registration_entity_id + ".notification",
				registration_entity_id + ".notification", "#", queue_bind_handler -> {
			if(queue_bind_handler.succeeded()) {
				client.queueBind(registration_entity_id + ".private",
						registration_entity_id + ".private", "#", private_queue_bind_handler -> {
					if(private_queue_bind_handler.succeeded()) {
						client.queueBind("database", registration_entity_id + ".private", "#", database_private_bind_handler -> {
							if(database_private_bind_handler.succeeded()) {
								client.queueBind("database", registration_entity_id + ".public", "#", database_public_bind_handler -> {
									if(database_public_bind_handler.succeeded()) {
										client.queueBind("database", registration_entity_id + ".protected", "#", database_protected_bind_handler -> {
											if(database_protected_bind_handler.succeeded()) {
												client.queueBind("database", registration_entity_id + ".notification", "#", database_notification_bind_handler -> {
													if(database_notification_bind_handler.succeeded()) {
														client.queueBind("database", registration_entity_id + ".publish", "#", database_publish_bind_handler -> {
															if(database_publish_bind_handler.succeeded()) {
																client.queueBind("database", registration_entity_id + ".diagnostics", "#", database_diagnostics_bind_handler -> {
																	if(database_diagnostics_bind_handler.succeeded()) {
																		createBindings.complete();
																																					}
																																				});
																															}
																														});
																									}
																								});
																					}
																				});
																	}
																});
													}
												});
									}
								});
					}
				});
		return createBindings;
	}
	
	
	/**
	 * This method is the implementation of Publish API, which handles the
	 * publication request by clients.
	 * 
	 * @param HttpServerRequest event - This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp - This sends the appropriate response for the
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
	 * @param String connection_pool_id - This is the key for the ConcurrentHashMap.
	 *               The id is used to map the created connection to a
	 *               RabbitMQClient.
	 * @param String username - This is the username to be used for the request to
	 *               RabbitMQ.
	 * @param String password - This is the the password to be used for the request to
	 *               RabbitMQ
	 * @return Future<RabbitMQClient> create_broker_client - This returns a Future
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

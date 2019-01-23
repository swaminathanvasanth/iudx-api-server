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

package iudx.http;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.hash.Hashing;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgIterator;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.Tuple;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import io.vertx.serviceproxy.ServiceProxyBuilder;
import iudx.URLs;
import iudx.broker.BrokerService;
import iudx.database.DbService;

/**
 * <h1>IUDX API Server</h1> An Open Source implementation of India Urban Data
 * Exchange (IUDX) platform APIs using Vert.x, an event driven and non-blocking
 * high performance reactive framework, for enabling seamless data exchange in
 * Smart Cities.
 * 
 * @author Swaminathan Vasanth Rajaraman <swaminathanvasanth.r@gmail.com>
 * @version 1.0.0
 */

public class HttpServerVerticle extends AbstractVerticle implements  Handler<HttpServerRequest>
{
	private final static Logger logger = Logger.getLogger(HttpServerVerticle.class.getName());

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
	
	private boolean 		login_success;
	private JsonObject 		queryObject;
	private boolean 		autonomous;
	private String 			schema;
	private DbService 		dbService;
	private BrokerService	brokerService;

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
	
	Future<Void> entity_verification;

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
	
	private static final String PASSWORDCHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-";
	
	
	// Used in registration API for apikey generation
	/**  Characters to be used by APIKey generator while generating apikey */
	
	/** Handles the generated apikey for the HTTP Registration API request */
	private String generated_apikey;
	/** Handles the generated apikey hash for the HTTP Registration API request */
	private String apikey_hash;
	/** Handles the generated apikey hash (in bytes) for the HTTP Registration API request */
	private byte[] hash;
	/** A MessageDigest object used for creating the apikey hash */
	
	// Used in subscribe API
	/**  Handles the read data object for HTTP subscribe request */
	private JsonObject json;
	/**  Handles the response data object for HTTP subscribe request */
	private JsonObject response;
	/**  Handles the response data array for HTTP subscribe request */
	private JsonArray array;
	/** Handles the data read from RabbitMQ queue for the HTTP subscribe API request */
	private String data;
	/** Handles the sender information of the data read from RabbitMQ queue for the HTTP subscribe API request */
	private String from;
	/** Handles the topic of the data read from RabbitMQ queue for the HTTP subscribe API request */
	private String routing_key;
	/** Handles the content type of the data read from RabbitMQ queue for the HTTP subscribe API request */
	private String content_type;
	/** A boolean variable (FLAG) used for handling the state of the HTTP subscribe API request */
	private boolean response_written = false;
	/** A boolean variable (FLAG) used for handling the message_type in the subscribe API request */
	private boolean default_message_type = false;
	/** An integer variable used for handling the number of messages requested for a subscribe API request */
	private int num_messages = 0;
	/** An integer variable used for handling the number of messages to be read from RabbitMQ for a subscribe API request */
	private int count = 0;
	/** An integer variable used for handling the number of messages read from RabbitMQ for a subscribe API request */
	private int read = 0;
	
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
	public void start(Future<Void> startFuture) throws Exception 
	{
		/**  Defines the port at which the apiserver should run */
		
		logger.setLevel(Level.INFO);
		
		int port 			= 	8443;
		
		broker_url			= 	URLs.getBrokerUrl();
		broker_port 		= 	URLs.getBrokerPort();
		broker_vhost 		= 	URLs.getBrokerVhost();
		broker_username 	= 	URLs.getBrokerUsername();
		broker_password		= 	URLs.getBrokerPassword();
		
		queryObject			= 	new JsonObject();
		login_success		= 	false;
		autonomous 			= 	false;
		
		dbService			=	DbService.createProxy(vertx, "db.queue");
		brokerService		=	BrokerService.createProxy(vertx, "broker.queue");
		
		HttpServer server 	= vertx.createHttpServer(new HttpServerOptions()
									.setSsl(true)
									.setKeyStoreOptions(new JksOptions()
									.setPath("my-keystore.jks")
									.setPassword("password")));
		
		server
			.requestHandler(HttpServerVerticle.this)
			.listen(port, ar -> {
				
				if(ar.succeeded())
				{
					startFuture.complete();
				}
				else
				{
					startFuture.fail(ar.cause());
				}
			});

		vertx.exceptionHandler(err -> {
			err.printStackTrace();
		});
		
	}
	
	@Override
	public void stop() 
	{
		if (server != null)
		{
			server.close();
		}
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
	public void handle(HttpServerRequest event) 
	{
		switch (event.path()) 
		{
			case "/admin/register-owner":
			
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					register_owner(event);
					break;
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
	
			case "/admin/deregister-owner":
			
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					de_register_owner(event);
				}
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
		
			case "/entity/publish":
				publish(event);
				break;
		
			case "/entity/subscribe":
				subscribe(event);
				break;
		
			case "/owner/register-entity":
			case "/admin/register-entity":
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					register(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
			
			case "/owner/deregister-entity":
			case "/admin/deregister-entity":
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					de_register(event);
				}
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/owner/block":
			case "/admin/block":
			
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					block(event, "t");
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/owner/unblock":
			case "/admin/unblock":
			
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					block(event, "f");
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
			
			default:
				resp = event.response();
				resp.setStatusCode(404).end();
				break;
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
	
	private void register_owner(HttpServerRequest req) 
	{
		HttpServerResponse resp		=	req.response();
		String id					=	req.getHeader("id");
		String apikey				=	req.getHeader("apikey");
		String owner_name			=	req.getHeader("owner");
		
		if(		(id == null)
					||
			(apikey == null)
					||
			(owner_name == null)
		)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
		}
		
		if (!id.equalsIgnoreCase("admin")) 
		{
			resp.setStatusCode(403).end();
			return;
		}
		
		if(!is_valid_owner(owner_name))
		{
			resp.setStatusCode(400).end("Owner name is invalid");
		}
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			entity_does_not_exist(owner_name)
			.setHandler(entityDoesNotExist -> {
					
		if(entityDoesNotExist.succeeded())
		{
			generate_credentials(owner_name, "{}", "true")
			.setHandler(generate_credentials -> {
								
		if(generate_credentials.succeeded())
		{
			brokerService.create_owner_resources(owner_name, broker_create -> {
											
		if(broker_create.succeeded())
		{
			brokerService.create_owner_bindings(owner_name, broker_bind -> {
			
		if(broker_bind.succeeded())
		{
			JsonObject response = new JsonObject();
			response.put("id", owner_name);
			response.put("apikey", generate_credentials.result());
			resp.setStatusCode(200).end(response.toString());			
		}
		else
		{
			resp.setStatusCode(500).end("Could not create bindings");
		}
			
	});		
		}
		else
		{
			resp.setStatusCode(500).end("Could not create exchanges and queues");
		}	
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not generate credentials");
		}
								
	});
		}
		else
		{
			resp.setStatusCode(409).end("Owner already exists");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
		}
	});
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
	
	//TODO: deregister owner has to be async
	//TODO: Handle all errors correctly
	//TODO: Add script to remove zombie entries in postgres as well as broker (in case async deregister fails)
	
	private void de_register_owner(HttpServerRequest req) 
	{
		HttpServerResponse resp		= req.response();
		String id 					= req.getHeader("id");
		String apikey 				= req.getHeader("apikey");
		String owner_name 			= req.getHeader("owner");
		
		if(		(id == null)
					||
			  (apikey == null)
					||
			(owner_name == null)
		)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
		}
		
		if (!id.equalsIgnoreCase("admin")) 
		{
			resp.setStatusCode(403).end();
			return;
		}
		
		if(!is_valid_owner(owner_name))
		{
			resp.setStatusCode(400).end("Owner name is invalid");
		}
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			entity_exists(owner_name).setHandler(ownerExists -> {
					
		if(ownerExists.succeeded())
		{
			brokerService.delete_owner_resources(owner_name, delete_owner_resources -> {
							
		if(delete_owner_resources.succeeded())
		{
			String acl_query	=	"DELETE FROM acl WHERE"	+
									" from_id LIKE '"		+	
									owner_name				+	
									"/%%'"					+
									" OR exchange LIKE '"	+
									owner_name				+
									"/%%'"					;
								
			dbService.runQuery(acl_query, aclDelete -> {
									
		if(aclDelete.succeeded())
		{
			String entity_query	=	"SELECT * FROM users WHERE"	+
						    		" id LIKE '"				+	
						    		owner_name					+								    					  "/%%'";
									
			String columns		=	"id";
										
			dbService.selectQuery(entity_query, columns, ids -> {
											
		if(ids.succeeded())
		{			
			String id_list	=	ids.result().getJsonObject("result").getString("id");
												
			brokerService.delete_entity_resources(id_list, deleteEntities -> {
												
		if(deleteEntities.succeeded())
		{
			String user_query	=	"DELETE FROM users WHERE"	+
									" id LIKE '"				+	
									owner_name					+	
									"/%%'"						+
									" OR id LIKE '"				+
									owner_name					+
									"'"							;
														
			dbService.runQuery(user_query, deleteUsers -> {
															
		if(deleteUsers.succeeded())
		{
			resp.setStatusCode(200).end();
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from users' table");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete owner entities");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not get entities belonging to owner");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from acl table");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete owner resources");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such owner");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
		}
			
	});
	
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

	//TODO: Try Future Compose?
	private void register(HttpServerRequest req) 
	{
		HttpServerResponse resp	=	req.response();
		String id 				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");
		String is_autonomous	=	req.getHeader("is-autonomous");
		String full_entity_name	=	id	+ "/" 	+ entity;
		
		String autonomous_flag;
		
		if(is_autonomous	==	null)
		{
			autonomous_flag	=	"f";
		}
		else if(is_autonomous.equals("true"))
		{
			autonomous_flag	=	"t";
		}
		else if(is_autonomous.equals("false"))
		{
			autonomous_flag	=	"f";
		}
		else
		{
			resp.setStatusCode(400).end("Invalid is-autonomous header");
			return;
		}
		
		//TODO: Check if body is null
		req.bodyHandler(body -> {
			schema = body.toString();
		});	
		
		if(	  (id == null)
				  ||
			(apikey == null)
				  ||
			(entity == null)
		)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		//TODO: Add appropriate field checks. E.g. valid owner, valid entity etc.
		// Check if ID is owner
		if (!is_valid_owner(id)) 
		{
			resp.setStatusCode(403).end("Invalid owner");
			return;
		} 
		
		if(!is_string_safe(entity))
		{
			resp.setStatusCode(400).end("Invalid entity name");
			return;
		}
		
		check_login(id,apikey).setHandler(login -> {
			
		if(login.succeeded())
		{
			entity_does_not_exist(full_entity_name).setHandler(entityDoesNotExist -> {
					
		if(entityDoesNotExist.succeeded())
		{
			generate_credentials(full_entity_name, schema, autonomous_flag)
			.setHandler(genCredentials -> {
							
		if(genCredentials.succeeded())
		{
			brokerService.create_entity_resources(full_entity_name, createEntityResources -> {
									
		if(createEntityResources.succeeded())
		{
			brokerService.create_entity_bindings(full_entity_name, createEntityBindings -> {
										
		if(createEntityBindings.succeeded())
		{
			JsonObject response = new JsonObject();
			response.put("id", full_entity_name);
			response.put("apikey", genCredentials.result());
			resp.setStatusCode(200).end(response.toString());
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not create bindings");
		}		
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not create exchanges and queues");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not generate entity credentials");
		}
	});
		}
		else
		{
			resp.setStatusCode(409).end("ID already used");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
		}
	});	
}
	/**
	 * This method is the implementation of entity De-Registration API, which handles
	 * the device or application de-registration requests by owners.
	 * 
	 * @param HttpServerRequest req - This is the handle for the incoming request
	 *                          from client.
	 * @return HttpServerResponse resp - This sends the appropriate response for the
	 *         incoming request.
	 */
	
	private void de_register(HttpServerRequest req) 
	{
		HttpServerResponse resp =	req.response();
		String id 				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");

		// Check if ID is owner
		if (!is_valid_owner(id)) 
		{
			resp.setStatusCode(400).end("Invalid owner");
			return;
		} 
		
		if(!is_valid_entity(entity))
		{
			resp.setStatusCode(403).end("Invalid entity");
			return;
		}
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			entity_exists(entity)
			.setHandler(entityExists -> {
					
		if(entityExists.succeeded())
		{
			brokerService.delete_entity_resources(entity, deleteEntityResources -> {
							
		if(deleteEntityResources.succeeded())
		{
			String acl_query	=	"DELETE FROM acl WHERE "	+
									"from_id = '"				+
									entity						+
									"' OR exchange LIKE '"		+
									entity						+
									".%%'"						;
			dbService.runQuery(acl_query, aclQuery -> {
									
		if(aclQuery.succeeded())
		{
			String follow_query	=	"DELETE FROM follow WHERE "	+
									" requested_by = '"			+
									entity						+
									"' OR exchange LIKE '"		+
									entity						+
									".%%'"						;
										
			dbService.runQuery(follow_query, followQuery -> {
											
		if(followQuery.succeeded())
		{
			String user_query	=	"DELETE FROM users WHERE "	+
									" id = '"					+
									entity						+
									"'"							;
												
			dbService.runQuery(user_query, userQuery -> {
													
		if(userQuery.succeeded())
		{
			resp.setStatusCode(200).end();
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from users");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from follow");
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from acl");
		}	
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete exchanges and queues");
		}
	});
		}
		else
		{
			resp.setStatusCode(400).end("No such entity");
		}
					
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
		}
	});
}
	/**
	 * This method is the implementation of entity Block and Un-Block API, which
	 * handles the device or application block requests by owners.
	 * 
	 * @param HttpServerRequest req - This is the handle for the incoming request
	 *                          from client.
	 * @param                   boolean block - This is the flag for a block request
	 * @param                   boolean un_block - This is the flag for an un-block
	 *                          request
	 * @return HttpServerResponse resp - This sends the appropriate response for the
	 *         incoming request.
	 */
	
	private void block(HttpServerRequest req, String blocked) 
	{
		HttpServerResponse resp	=	req.response();
		String id				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");
		
		if(   
			(id == null)
				  ||
			(apikey == null)
				  ||
			(entity == null)
		)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if(!is_valid_owner(id))
		{
			resp.setStatusCode(400).end("Invalid owner");
			return;
		}
		
		if(!is_valid_entity(entity))
		{
			resp.setStatusCode(400).end("Invalid entity");
			return;
		}
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			entity_exists(entity)
			.setHandler(entityExists -> {
					
		if(entityExists.succeeded())
		{
			String query	=	"UPDATE users SET blocked = '"	+
								blocked							+
								"' WHERE id ='"					+
								entity							+
								"'"								;
			dbService.runQuery(query, updateUserTable -> {
							
		if(updateUserTable.succeeded())
		{
			resp.setStatusCode(200).end();
		}
		else
		{
			resp.setStatusCode(500).end("Could not update users table");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such entity");
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
		}
	});
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
	
	private Future<Void> entity_exists(String registration_entity_id) 
	{
		Future<Void> future	=	Future.future();		
		String query		=	"SELECT * FROM users WHERE id = '"
								+ registration_entity_id +	"'";
		String columns		=	"id";
		
		queryObject.put("query", query);
		queryObject.put("columns", columns);
		
		dbService.selectQuery(query, columns, reply -> {
			
		if(reply.succeeded())
		{
			if(reply.result().getInteger("rowCount")>0)
			{
				future.complete();
			}
			else
			{
				future.fail("Entity does not exist");
			}
				
		}
	});
		return future;
	}
	
	private Future<Void> entity_does_not_exist(String registration_entity_id) 
	{
		Future<Void> future		=	Future.future();		
		String query			=	"SELECT * FROM users WHERE id = '"
									+ registration_entity_id +	"'";
		String columns			=	"id";
		
		queryObject.put("query", query);
		queryObject.put("columns", columns);
		
		dbService.selectQuery(query, columns, reply -> {
			
		if(reply.succeeded())
		{
			if(reply.result().getInteger("rowCount")>0)
			{
				future.fail("Entity exists");
				
			}
			else
			{
				future.complete();
			}
				
		}
	});
		return future;
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
	 * This method is the implementation of Subscribe API, which handles the
	 * subscription request by clients.
	 * 
	 * @param HttpServerRequest event - This is the handle for the incoming request
	 *                          from client.
	 * @return void - Though the return type is void, the HTTP response is written internally as per the request. 
	 */
	
	private void subscribe(HttpServerRequest request) 
	{
		array 	= new JsonArray();
		resp 	= request.response();
		
		resp.setChunked(true);
		
		requested_id 		= request.getHeader("id");
		requested_apikey 	= request.getHeader("apikey");
		message_type 		= request.getHeader("message-type");
		
		if (message_type == null) 
		{
			message_type = "";
			default_message_type = true;
		} 
		else 
		{
			message_type = ("." + message_type).trim();
			default_message_type = false;
		}
		if ((message_type.equalsIgnoreCase(".priority")) 
							|| 
			(message_type.equalsIgnoreCase(".command"))
							|| 
			(message_type.equalsIgnoreCase(".notification"))
							|| 
			(message_type.equalsIgnoreCase(""))
			)
		{
			count = read = 0;
			response_written = false;
			
			try 
			{
				num_messages = Integer.parseInt(request.getHeader("num-messages"));
				
				if (num_messages > 100) 
				{
					num_messages = 100;
				}
			} 
			catch (Exception e) 
			{
				num_messages = 10;
			}

			connection_pool_id = requested_id + requested_apikey;

			if (!rabbitpool.containsKey(connection_pool_id)) 
			{
				broker_client = getRabbitMQClient(connection_pool_id, requested_id, requested_apikey);
				broker_client.setHandler(broker_client_start_handler -> {
					
				for (count = 1; count <= num_messages; count++) 
				{
					Future<RabbitMQClient> completedgetData = getData(broker_client_start_handler);
					completedgetData.setHandler(completed_getData_handler -> {
					
					if (completed_getData_handler.succeeded()) 
					{
						resp.setStatusCode(200).write(array + "\r\n").end();
						response_written = true;
						return;
					}
					});
				}
				});
			}
		} 
		else 
		{
			response = new JsonObject();
			response.put("error", "invalid message-type");
			resp.setStatusCode(400).write(response + "\r\n").end();
			response_written = true;
			return;
		}
	}

	/**
	 * This method is used to get data from RabbitMQ queues.
	 * 
	 * @param AsyncResult<RabbitMQClient> broker_client_start_handler - This is the client handler for connecting with the RabbitMQ.
	 * @return Future<RabbitMQClient> getData - This is a callable Future which
	 *         notifies the caller on completion of reading data from queue.
	 */
	
	private Future<RabbitMQClient> getData(AsyncResult<RabbitMQClient> broker_client_start_handler) {
		Future<RabbitMQClient> getData = Future.future();
		if (broker_client_start_handler.succeeded()) {
			if (default_message_type) {
				default_message_type = false;
				rabbitpool.get(connection_pool_id).basicGet(requested_id, true, queue_handler -> {
					if (queue_handler.succeeded()) {
						json = (JsonObject) queue_handler.result();
						try {
							data = json.getString("body");
							from = json.getString("exchange");
							System.out.println(from + from.length());
							routing_key = json.getString("routingKey");
							content_type = json.getString("content-type");

							if (data == null || data == "" || data.length() == 0) {
								data = "<empty>";
							}

							if (from == null || from == "" || from.length() == 0) {
								from = "<unverified>";
							}

							if (routing_key == null || routing_key == "" || routing_key.length() == 0) {
								routing_key = "<unverified>";
							}

							if (content_type == null || content_type == "" || content_type.length() == 0) {
								content_type = "<unverified>";
							}

							response = new JsonObject();
							response.put("data", data);
							response.put("from", from);
							response.put("topic", routing_key);
							response.put("content-type", content_type);
							array.add(response);
							read = read + 1;
							if (read == num_messages) {
								getData.complete();
							}
						} catch (Exception ex) {
							if (!response_written) {
								resp.setStatusCode(200).write(array + "\r\n").end();
								response_written = true;
								return;
							}
						}
					}
				});
			} else {
				rabbitpool.get(connection_pool_id).basicGet(requested_id + message_type, true, queue_handler -> {
					if (queue_handler.succeeded()) {
						json = (JsonObject) queue_handler.result();
						try {
							data = json.getString("body");
							from = json.getString("exchange");
							System.out.println(from + from.length());
							routing_key = json.getString("routingKey");
							content_type = json.getString("content-type");

							if (data == null || data == "" || data.length() == 0) {
								data = "<empty>";
							}

							if (from == null || from == "" || from.length() == 0) {
								from = "<unverified>";
							}

							if (routing_key == null || routing_key == "" || routing_key.length() == 0) {
								routing_key = "<unverified>";
							}

							if (content_type == null || content_type == "" || content_type.length() == 0) {
								content_type = "<unverified>";
							}

							response = new JsonObject();
							response.put("data", data);
							response.put("from", from);
							response.put("topic", routing_key);
							response.put("content-type", content_type);
							array.add(response);
							read = read + 1;
							if (read == num_messages) {
								getData.complete();
							}
						} catch (Exception ex) {
							if (!response_written) {
								resp.setStatusCode(200).write(array + "\r\n").end();
								response_written = true;
								return;
							}
						}
					}
				});
			}
		}
		return getData;
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

	public Future<RabbitMQClient> getRabbitMQClient(String connection_pool_id, String username, String password) 
	{
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
		
		if (start_handler.succeeded()) 
		{
			rabbitpool.put(connection_pool_id, client);
			create_broker_client.complete();
		}
	});

		 return create_broker_client;		
	}
	
	public Future<String> generate_credentials(String id, String schema, String autonomous) 
	{
		Future<String> create_credentials = Future.future();
		
		String apikey	=	genRandString(32);
		String salt 	=	genRandString(32);
		String blocked 	=	"f";
		
		String string_to_hash	=	apikey + salt + id;
		String hash				=	Hashing.sha256()
									.hashString(string_to_hash, StandardCharsets.UTF_8)
									.toString();
		
		String query = "INSERT INTO users VALUES('"
						+id			+	"','"
						+hash		+	"','"
						+schema 	+	"','"
						+salt		+ 	"','"
						+blocked	+	"','"
						+autonomous +	"')";
		
		dbService.runQuery(query, reply -> {
			
		if(reply.succeeded())
		{
			create_credentials.complete(apikey);
		}
		else
		{
			create_credentials.fail(reply.cause().toString());
		}

	});
	
		return create_credentials;
	}
	
	public String genRandString(int len)
	{
		String randStr = RandomStringUtils.random(len, 0, PASSWORDCHARS.length(), true, true, PASSWORDCHARS.toCharArray());
		return randStr;
	}
	
	public Future<Void> check_login(String id, String apikey)
	{
		Future<Void> check = Future.future();

		if(id.equalsIgnoreCase("") || apikey.equalsIgnoreCase(""))
		{
			check.fail("Invalid credentials");
		}
		
		//TODO check if id conforms to the required format
		if(!id.matches("[a-z0-9/]+"))
		{
			check.fail("Invalid credentials");
		}
		
		String query		=	"SELECT * FROM users WHERE id = '"
								+	id	+ "'"
								+	"AND blocked = 'f'";
		
		String columns		=	"salt,password_hash,is_autonomous";
		
		dbService.selectQuery(query, columns, reply -> {
			
		if(reply.succeeded())
		{
			JsonObject queryResult = reply.result();
				
			if(queryResult.getInteger("rowCount")==0)
			{
				check.fail("Not found");
				check.complete();
			}
			else
			{
				JsonObject result 		=	queryResult.getJsonObject("result");
					
				String salt 			=	result.getString("salt");
				String string_to_hash	=	apikey	+	salt	+	id;
				String expected_hash 	= 	result.getString("password_hash");
				String actual_hash 		= 	Hashing
											.sha256()
											.hashString(string_to_hash, StandardCharsets.UTF_8)
											.toString();
										
				if(actual_hash.equals(expected_hash))
				{
					check.complete();
				}
				else
				{
					check.fail("Invalid credentials");
				}
			}
		}
	});
		
	return check;
	}

	public boolean is_string_safe(String resource)
	{
		boolean safe = (resource.length() - (resource.replaceAll("[^-/a-zA-Z0-9]+", "")).length())==0?true:false;
		System.out.println("Replaced = " + resource.replaceAll("[^-/a-zA-Z0-9]+", "") );
		return safe;
	}
	
	public boolean is_valid_owner(String owner_name)
	{
		if	(	(!Character.isDigit(owner_name.charAt(0)))
									&&
			(	(owner_name.length() - (owner_name.replaceAll("[^a-z0-9]+", "")).length())==0)
			)
		{
			return true;
		}
		else
		{
			logger.info("Name = " + owner_name + " Replaced = " + owner_name.replaceAll("[^a-z0-9]+", ""));
			return false;
		}
	}
	
	public boolean is_valid_entity(String resource)
	{
		String entries[]	=	resource.split("/");
		
		System.out.println("Entries = " + Arrays.asList(entries));
		
		if(entries.length!=2)
		{
			return false;
		}
		else if	(	(is_valid_owner(entries[0]))
								&&
					(is_string_safe(entries[1]))
				)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
}

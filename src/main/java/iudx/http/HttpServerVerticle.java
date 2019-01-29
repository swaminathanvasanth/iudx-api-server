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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.RandomStringUtils;

import com.google.common.hash.Hashing;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgIterator;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.reactiverse.pgclient.Tuple;

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
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
	private final static Logger logger = LoggerFactory.getLogger(HttpServerVerticle.class);

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
	
	private String 			schema;
	private DbService 		dbService;
	private BrokerService	brokerService;
	
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
	
	//Characters to be used by APIKey generator while generating apikey 
	private static final String PASSWORDCHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-";
	
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
		logger.debug("In start");
		
		int port 			= 	8443;
		
		broker_url			= 	URLs.getBrokerUrl();
		broker_port 		= 	URLs.getBrokerPort();
		broker_vhost 		= 	URLs.getBrokerVhost();
		broker_username 	= 	URLs.getBrokerUsername();
		broker_password		= 	URLs.getBrokerPassword();
		
		dbService			=	DbService.createProxy(vertx, "db.queue");
		brokerService		=	BrokerService.createProxy(vertx, "broker.queue");
		
		HttpServer server 	=	vertx.createHttpServer(new HttpServerOptions()
								.setSsl(true)
								.setKeyStoreOptions(new JksOptions()
								.setPath("my-keystore.jks")
								.setPassword("password")));
		
		server
		.requestHandler(HttpServerVerticle.this)
		.listen(port, ar -> {
				
			if(ar.succeeded())
			{
				logger.debug("Server started");
				startFuture.complete();
			}
			else
			{
				logger.debug("Could not start server. Cause="+ar.cause());
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
		logger.debug("In handle method");
		
		logger.debug("Event path="+event.path());
		
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

			case "/admin/block_owner":
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					block_owner(event, "t");
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;

			case "/admin/unblock_owner":
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					block_owner(event, "f");
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
				
			case "/entity/bind":
			case "/owner/bind" :
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					queue_bind(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/entity/share":
			case "/owner/share" :
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					share(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/owner/entities" :
				
				if(event.method().toString().equalsIgnoreCase("GET")) 
				{
					entities(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/owner/reset-apikey" :
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					reset_apikey(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
			
			case "/owner/set-autonomous" :
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					set_autonomous(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
			
			case "/admin/owners" :
				
				if(event.method().toString().equalsIgnoreCase("GET")) 
				{
					owners(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/entity/follow-requests":
			case "/owner/follow-requests" :
				
				if(event.method().toString().equalsIgnoreCase("GET")) 
				{
					follow_requests(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/entity/follow-status":
			case "/owner/follow-status" :
				
				if(event.method().toString().equalsIgnoreCase("GET")) 
				{
					follow_status(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/entity/reject-follow":
			case "/owner/reject-follow" :
				
				if(event.method().toString().equalsIgnoreCase("POST")) 
				{
					reject_follow(event);
				} 
				else 
				{
					resp = event.response();
					resp.setStatusCode(404).end();
				}
				break;
				
			case "/entity/permissions":
			case "/owner/permissions" :
				
				if(event.method().toString().equalsIgnoreCase("GET")) 
				{
					permissions(event);
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
		logger.debug("In register owner");
		
		HttpServerResponse resp		=	req.response();
		String id					=	req.getHeader("id");
		String apikey				=	req.getHeader("apikey");
		String owner_name			=	req.getHeader("owner");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("owner="+owner_name);
		
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
			logger.debug("Login ok");
			
			entity_does_not_exist(owner_name)
			.setHandler(entityDoesNotExist -> {
					
		if(entityDoesNotExist.succeeded())
		{
			logger.debug("Owner does not exist");
			
			//TODO schema is null
			generate_credentials(owner_name, "{}", "true")
			.setHandler(generate_credentials -> {
								
		if(generate_credentials.succeeded())
		{
			logger.debug("Generated credetials for owner");
			
			brokerService.create_owner_resources(owner_name, broker_create -> {
											
		if(broker_create.succeeded())
		{
			logger.debug("Created owner resources");
			
			brokerService.create_owner_bindings(owner_name, broker_bind -> {
			
		if(broker_bind.succeeded())
		{
			logger.debug("Created owner bindings. All ok");
			
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
		logger.debug("In deregister_owner");
		
		HttpServerResponse resp		= req.response();
		String id 					= req.getHeader("id");
		String apikey 				= req.getHeader("apikey");
		String owner_name 			= req.getHeader("owner");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("owner="+owner_name);
		
		if(		(id == null)
					||
			  (apikey == null)
					||
			(owner_name == null)
		)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
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
			logger.debug("Login ok");
			entity_exists(owner_name).setHandler(ownerExists -> {
					
		if(ownerExists.succeeded())
		{
			logger.debug("Owner exists");
			
			brokerService.delete_owner_resources(owner_name, delete_owner_resources -> {
							
		if(delete_owner_resources.succeeded())
		{
			logger.debug("Deleted owner resources from broker");
			
			String acl_query	=	"DELETE FROM acl WHERE"	+
									" from_id LIKE '"		+	
									owner_name				+	
									"/%'"					+
									" OR exchange LIKE '"	+
									owner_name				+
									"/%'"					;
								
			dbService.runQuery(acl_query, aclDelete -> {
									
		if(aclDelete.succeeded())
		{
			logger.debug("Deleted owner entries from acl table");
			
			String entity_query	=	"SELECT * FROM users WHERE"	+
						    		" id LIKE '"				+	
						    		owner_name					+								    					  
						    		"/%'"						;
										
			dbService.runQuery(entity_query, ids -> {
											
		if(ids.succeeded())
		{	
			List<String> resultList	=	ids.result();
			
			String id_list	=	"";
			
			for(String row:resultList)
			{
				String processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
				
				logger.debug("Processed row="+Arrays.asList(processed_row));
				
				id_list	=	id_list	+	processed_row[0]	+	",";
			}
			
			id_list	=	id_list.substring(0,id_list.length()-1);
			
			logger.debug("id_list="+id_list);
												
			brokerService.delete_entity_resources(id_list, deleteEntities -> {
												
		if(deleteEntities.succeeded())
		{
			logger.debug("Deleted entity resources from broker");
			
			String user_query	=	"DELETE FROM users WHERE"	+
									" id LIKE '"				+	
									owner_name					+	
									"/%'"						+
									" OR id LIKE '"				+
									owner_name					+
									"'"							;
														
			dbService.runQuery(user_query, deleteUsers -> {
															
		if(deleteUsers.succeeded())
		{
			logger.debug("Deleted entities from users table. All ok");
			
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from users' table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete owner entities");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not get entities belonging to owner");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete from acl table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not delete owner resources");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such owner");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
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
		logger.debug("In register entity");
		
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
		
		logger.debug("id="+id+"\napikey="+apikey+"\nentity="+entity+"\nis-autonomous="+autonomous_flag);
		
		//TODO: Check if body is null
		req.bodyHandler(body -> {
			schema = body.toString();
			logger.debug("schema="+schema);
			try
			{
				JsonObject test	=	new JsonObject(schema);
			}
			catch (Exception e)
			{
				resp.setStatusCode(403).end("Body must be a valid JSON");
				return;
			}
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
			logger.debug("owner is invalid");
			resp.setStatusCode(403).end("Invalid owner");
			return;
		} 
		
		if(!is_string_safe(entity))
		{
			logger.debug("invalid entity name");
			resp.setStatusCode(400).end("Invalid entity name");
			return;
		}
		
		check_login(id,apikey).setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("login ok");
			entity_does_not_exist(full_entity_name).setHandler(entityDoesNotExist -> {
					
		if(entityDoesNotExist.succeeded())
		{
			logger.debug("entity does not exist");
			generate_credentials(full_entity_name, schema, autonomous_flag)
			.setHandler(genCredentials -> {
							
		if(genCredentials.succeeded())
		{
			logger.debug("credentials generated");
			brokerService.create_entity_resources(full_entity_name, createEntityResources -> {
									
		if(createEntityResources.succeeded())
		{
			logger.debug("resources created");
			brokerService.create_entity_bindings(full_entity_name, createEntityBindings -> {
										
		if(createEntityBindings.succeeded())
		{
			logger.debug("all ok");
			JsonObject response = new JsonObject();
			response.put("id", full_entity_name);
			response.put("apikey", genCredentials.result());
			resp.setStatusCode(200).end(response.toString());
			return;
		}
		else
		{
			logger.debug(createEntityBindings.cause());
			resp.setStatusCode(500).end("Could not create bindings");
			return;
		}		
	});
		}
		else
		{
			logger.debug(createEntityResources.cause());
			resp.setStatusCode(500).end("Could not create exchanges and queues");
			return;
		}
	});
		}
		else
		{
			logger.debug(genCredentials.cause());
			resp.setStatusCode(500).end("Could not generate entity credentials");
			return;
		}
	});
		}
		else
		{
			logger.debug(entityDoesNotExist.cause());
			resp.setStatusCode(409).end("ID already used");
			return;
		}
	});
		}
		else
		{
			logger.debug(login.cause());
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
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
		logger.debug("In deregister entity");
		
		HttpServerResponse resp =	req.response();
		String id 				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");
		
		logger.debug("id="+id+"\napikey="+apikey+"\nentity="+entity);

		// Check if ID is owner
		if (!is_valid_owner(id)) 
		{
			resp.setStatusCode(400).end("Invalid owner");
			return;
		} 
		
		if(!is_owner(id, entity))
		{
			resp.setStatusCode(403).end("You are not the owner of the entity");
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
			logger.debug("login ok");
			
			entity_exists(entity)
			.setHandler(entityExists -> {
					
		if(entityExists.succeeded())
		{
			logger.debug("entity exists");
			brokerService.delete_entity_resources(entity, deleteEntityResources -> {
							
		if(deleteEntityResources.succeeded())
		{
			logger.debug("Entity resources deleted");
			
			String acl_query	=	"DELETE FROM acl WHERE "	+
									"from_id = '"				+
									entity						+
									"' OR exchange LIKE '"		+
									entity						+
									".%'"						;
			dbService.runQuery(acl_query, aclQuery -> {
									
		if(aclQuery.succeeded())
		{
			logger.debug("Deleted from acl");
			
			String follow_query	=	"DELETE FROM follow WHERE "	+
									" requested_by = '"			+
									entity						+
									"' OR exchange LIKE '"		+
									entity						+
									".%'"						;
										
			dbService.runQuery(follow_query, followQuery -> {
											
		if(followQuery.succeeded())
		{
			logger.debug("Deleted from follow");
			
			String user_query	=	"DELETE FROM users WHERE "	+
									" id = '"					+
									entity						+
									"'"							;
												
			dbService.runQuery(user_query, userQuery -> {
													
		if(userQuery.succeeded())
		{
			logger.debug("all ok");
			
			resp.setStatusCode(200).end();
		}
		else
		{
			logger.debug("Could not delete from users. Cause="+userQuery.cause());
			resp.setStatusCode(500).end("Could not delete from users");
		}
	});
		}
		else
		{
			logger.debug("Could not delete from follow. Cause="+followQuery.cause());
			resp.setStatusCode(500).end("Could not delete from follow");
		}
	});
		} 
		else // if (! aclQuery.succeeded())
		{
			logger.debug("Could not delete from acl. Cause="+aclQuery.cause());
			resp.setStatusCode(500).end("Could not delete from acl");
		}	
	});
		}
		else
		{
			logger.debug("Could not delete entity resources. Cause="+deleteEntityResources.cause());
			resp.setStatusCode(500).end("Could not delete exchanges and queues");
		}
	});
		}
		else
		{
			logger.debug("No such entity. Cause="+entityExists.cause());
			resp.setStatusCode(400).end("No such entity");
		}
					
	});
		}
		else
		{
			logger.debug("invalid credentials. Cause="+login.cause());
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
		logger.debug("In block/unblock API");
		
		HttpServerResponse resp	=	req.response();
		String id				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("entity="+entity);
		
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
		
		if(!is_owner(id, entity))
		{
			resp.setStatusCode(403).end("You are not the owner of the entity");
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
			logger.debug("Login ok");
			
			entity_exists(entity)
			.setHandler(entityExists -> {
					
		if(entityExists.succeeded())
		{
			logger.debug("Entity exists");
			
			String query	=	"UPDATE users SET blocked = '"	+
								blocked							+
								"' WHERE id ='"					+
								entity							+
								"'"								;
			dbService.runQuery(query, updateUserTable -> {
							
		if(updateUserTable.succeeded())
		{
			logger.debug("Updated users table. All ok");
			
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not update users table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such entity");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	
	private void block_owner(HttpServerRequest req, String blocked) 
	{
		logger.debug("In block/unblock owner API");
		
		HttpServerResponse resp	=	req.response();
		String id				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String entity			=	req.getHeader("entity");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("entity="+entity);
		
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
		
		
		if (!id.equalsIgnoreCase("admin")) 
		{
			resp.setStatusCode(403).end();
			return;
		}
		
		if(!is_valid_owner(entity))
		{
			resp.setStatusCode(400).end("Owner name is invalid");
		}

		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
			
			entity_exists(entity)
			.setHandler(entityExists -> {
					
		if(entityExists.succeeded())
		{
			logger.debug("Entity exists");
			
			String query	=	"UPDATE users SET blocked = '"	+
								blocked							+
								"' WHERE id ='"					+
								entity							+
								"'"								;
			dbService.runQuery(query, updateUserTable -> {
							
		if(updateUserTable.succeeded())
		{
			logger.debug("Updated users table. All ok");
			
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not update users table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such owner");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid credentials");
			return;
		}
	});
}
	
	private void queue_bind(HttpServerRequest req)
	{
		logger.debug("In queue_bind");
		
		HttpServerResponse resp	=	req.response();
		
		//Mandatory headers
		String id				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		String to				=	req.getHeader("to");
		String topic			=	req.getHeader("topic");
		String message_type		=	req.getHeader("message-type");
		
		//Optional headers
		String is_priority		=	req.getHeader("is-priority");
		String from				=	req.getHeader("from");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("to="+to);
		logger.debug("topic="+topic);
		logger.debug("message-type="+message_type);
		
		logger.debug("is-priorty="+is_priority);
		logger.debug("from="+from);
		
		if	(		(id		==	null)
							||
					(apikey	==	null)
							||
					(to		==	null)
							||
					(topic	==	null)
							||
				(message_type	==	null)
					
			)
			{
				resp.setStatusCode(400).end("Inputs missing in headers");
				return;
			}
		
		if(!(is_valid_owner(id) ^ is_valid_entity(id)))
		{
			resp.setStatusCode(400).end("Invalid id");
		}
		if(is_valid_owner(id))
		{
			if(from	==	null)
			{
				resp.setStatusCode(403).end("'from' value missing in headers");
				return;
			}
			
			if(!is_owner(id, from))
			{
				resp.setStatusCode(403).end("You are not the owner of the 'from' entity");
				return;
			}
			
			if(!is_valid_entity(from))
			{
				resp.setStatusCode(403).end("'from' is not a valid entity");
				return;
			}
		}
		else
		{
			from	=	id;
		}
		
		if	(	(!message_type.equals("public"))
							&&
				(!message_type.equals("private"))
							&&
				(!message_type.equals("protected"))
							&&
				(!message_type.equals("diagnostics"))
			)
		{
			resp.setStatusCode(400).end("'message-type' is invalid");
			return;
		}
		
		if	(	(message_type.equals("private"))	
							&&	
					(!is_owner(id, to))	
			)
		{
			resp.setStatusCode(403).end("You are not the owner of the 'to' entity");
			return;
		}
		
		if	(	(!is_string_safe(from))
						||
				(!is_string_safe(to))
						||
				(!is_string_safe(topic))
			)
		{
			resp.setStatusCode(403).end("Invalid headers");
			return;
		}
		
		String queue	=	from;
		
		if(is_priority	!=	null)
		{
			if	(	(!is_priority.equals("true")
								&&
					(!is_priority.equals("false")))
				)
			{
				resp.setStatusCode(403).end("Invalid is-priority header");
				return;
			}
			else if(is_priority.equals("true"))
			{
				queue	=	queue	+	".priority";
			}
		}
		
		final String from_id		=	from;
		final String exchange_name	=	to + "." + message_type;
		final String queue_name		=	queue;
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
			
			if(login.result())
			{
				logger.debug("Autonomous ok");
				
				if(!message_type.equals("public"))
				{
					logger.debug("Message type is not public");
					
					if(!is_owner(id, to))
					{
						logger.debug("Id is not the owner of to");
						
						String acl_query	=	"SELECT * FROM acl WHERE		"	+
												"from_id			=			'"	+
												from_id								+
												"' AND exchange		=			'"	+
												exchange_name						+
												"' AND permission	=	'read'	"	+
												" AND valid_till > now()		"	+
												" AND topic			=			'"	+
												topic								+
												"'"									;
			
						dbService.runQuery(acl_query, aclQuery -> {
				
					if(aclQuery.succeeded())
					{
						//TODO: Handle possible null pointer exception here
						if(aclQuery.result().size()==1)
						{
							brokerService.bind(queue_name, exchange_name, topic, bind -> {
							
								if(bind.succeeded())
								{
									logger.debug("Bound. All ok");
									
									resp.setStatusCode(200).end();
									return;
								}
								else
								{
									resp.setStatusCode(500).end("Bind failed");
									return;
								}
							});
						}
						else
						{
							resp.setStatusCode(403).end("Unauthorised");
							return;
						}
					}
					else
					{
						resp.setStatusCode(500).end("Could not query acl table");
						return;
					}
				});
					}
					else
					{
						logger.debug("Id is the owner of to");
						
						brokerService.bind(queue_name, exchange_name, topic, bind -> {
								
							if(bind.succeeded())
							{
								logger.debug("Bound. All ok");
								
								resp.setStatusCode(200).end();
								return;
							}
							else
							{
								resp.setStatusCode(500).end("Bind failed");
								return;
							}
				});
					}
						
				}
				else
				{
					logger.debug("Message type is public");
					
					brokerService.bind(queue_name, exchange_name, topic, bind -> {
							
						if(bind.succeeded())
						{
							logger.debug("Bound. All ok");
							
							resp.setStatusCode(200).end();
							return;
						}
						else
						{
							resp.setStatusCode(500).end("Bind failed");
							return;
						}
					});
				}
			}
			else
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}	
	});	
}
	
	private void share(HttpServerRequest req)
	{
		logger.debug("In share API");
		
		HttpServerResponse	resp	=	req.response();
		String 	id					=	req.getHeader("id");
		String 	apikey				=	req.getHeader("apikey");
		String 	follow_id			=	req.getHeader("follow-id");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("follow-id="+follow_id);
		
		
		if	(	(id	==	null)
						||
				(apikey	==	null)
						||
				(follow_id	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		String exchange_string	=	is_valid_owner(id)?(id+"/%.%"):(is_valid_entity(id))?id+".%":"";
		
		if(exchange_string	==	null)
		{
			resp.setStatusCode(403).end("Invalid id");
			return;
		}
		
		if(!is_string_safe(follow_id))
		{
			resp.setStatusCode(403).end("Invalid follow-id");
			return;
		}
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
			
			if(!login.result())
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
			
			logger.debug("Autonomous ok");
			
			String follow_query	=	"SELECT * FROM follow WHERE follow_id	=	"	+
									Integer.parseInt(follow_id)						+
									" AND exchange LIKE 						'"	+
									exchange_string									+
									"' AND status	=	'pending'				"	;
				
			dbService.runQuery(follow_query, getDetails -> {
					
		if(getDetails.succeeded())
		{
			List<String> resultList		=	getDetails.result();
			int rowCount				=	resultList.size();
			
			if(rowCount==1)
			{
				String row				=	resultList.get(0);
				
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
				
				String from_id			=	processed_row[1];
				String exchange			=	processed_row[2];
				String permission		=	processed_row[4];
				String topic			=	processed_row[5];
				String validity			=	processed_row[6];
				
				logger.debug("from_id="+from_id);
				logger.debug("exchange="+exchange);
				logger.debug("permission="+permission);
				logger.debug("topic="+topic);
				logger.debug("validity="+validity);
							
				String from_query		=	"SELECT * FROM users WHERE id = '" + from_id + "'";
							
				dbService.runQuery(from_query, fromQuery -> {
								
		if(fromQuery.succeeded())
		{
			logger.debug("From query ok");
			
			if(fromQuery.result().size()==1)
			{
				logger.debug("Found from entity");
				
				String update_follow_query	=	"UPDATE follow SET status = 'approved' WHERE follow_id = " + follow_id;
										
				dbService.runQuery(update_follow_query, updateFollowQuery -> {
											
		if(updateFollowQuery.succeeded())
		{
			logger.debug("Update follow ok");
			
			String acl_insert = "INSERT INTO acl VALUES("	+	"'"
								+from_id					+	"','"
								+exchange 					+	"','"
								+permission					+	"',"
								+"now() + interval '"		+
								Integer.parseInt(validity) 	+
								" hours'"					+	",'"
								+follow_id					+	"','"
								+topic						+	"',"
								+"DEFAULT)"					;
												
												
			dbService.runQuery(acl_insert, aclInsert -> {
													
		if(aclInsert.succeeded())
		{
			logger.debug("Insert into acl ok");
			
			if(permission.equals("write"))
			{
				logger.debug("Permission is write");
				
				String publish_exchange	=	from_id + ".publish";
				String publish_queue	=	exchange;
				String publish_topic	=	exchange + "." + topic;
															
				brokerService.bind(publish_queue, publish_exchange, publish_topic, bind -> {
																
		if(bind.succeeded())
		{
			logger.debug("Bound. All ok");
			
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Failed to bind");
			return;
		}
	});
		}
		else
		{
			logger.debug("All ok");
			
			resp.setStatusCode(200).end();
			return;
		}	
		}
		else
		{
			resp.setStatusCode(500).end("Failed to insert into acl table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not update follow table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid from id");
			return;
		}
		}
		else
		{
			resp.setStatusCode(500).end("Could not query users table");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("No such follow request with follow id = " + follow_id);
			return;
		}
		}
		else
		{
			resp.setStatusCode(500).end("Could not get follow details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
		
}

	public void entities(HttpServerRequest req)
	{
		logger.debug("In entities API");
		
		HttpServerResponse resp	=	req.response();
		
		String id				=	req.getHeader("id");
		String apikey			=	req.getHeader("apikey");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);

		if	(	(id	==	null)
						||
				(apikey	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if(!is_valid_owner(id))
		{
			resp.setStatusCode(403).end("id is not valid");
			return;
		}
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			if(!login.result())
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
				
			logger.debug("Autonomous ok");
				
			String users_query	=	"SELECT * FROM users WHERE id LIKE '" + id + "/%'";
			
			dbService.runQuery(users_query, query -> {
					
		if(query.succeeded())
		{
			List<String> resultList	=	query.result();
						
			JsonArray response		=	new JsonArray();
						
			for(String row:resultList)
			{
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
							
				logger.debug("Processed row ="+Arrays.asList(processed_row));
							
				JsonObject entity		=	new JsonObject();
							
				entity.put("id", processed_row[0]);
				entity.put("is-autonomous", processed_row[5]);
							
				response.add(entity);
			}
			
			logger.debug("All ok");
			
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
		else
		{
			resp.setStatusCode(500).end("Could not get entities' details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	public void reset_apikey(HttpServerRequest req)
	{
		logger.debug("In reset apikey");
		
		HttpServerResponse resp	=	req.response();
		
		String	id				=	req.getHeader("id");
		String 	apikey			=	req.getHeader("apikey");
		String	entity			=	req.getHeader("entity");
		
		if	(		(id	==	null)
						||
				(apikey	==	null)
						||
				(entity	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("entity="+entity);
		
		if(!is_valid_owner(id))
		{
			resp.setStatusCode(403).end("id is not valid");
			return;
		}
		
		if(!is_valid_entity(entity))
		{
			resp.setStatusCode(403).end("entity is not valid");
			return;
		}
		
		if(!is_owner(id, entity))
		{
			resp.setStatusCode(403).end("You are not the owner of the entity");
			return;
		}
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			update_credentials(entity)
			.setHandler(update -> {
					
		if(update.succeeded())
		{
			logger.debug("All ok");
						
			JsonObject response	=	new JsonObject();
						
			response.put("id", entity);
			response.put("apikey", update.result());
						
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
		else
		{
			resp.setStatusCode(500).end("Could not reset apikey");
			return;
		}
	});	
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	public void set_autonomous(HttpServerRequest req)
	{
		logger.debug("In set autonomous");
		
		HttpServerResponse	resp	=	req.response();
		String	id					=	req.getHeader("id");
		String	apikey				=	req.getHeader("apikey");
		String 	entity				=	req.getHeader("entity");
		String 	autonomous			=	req.getHeader("is-autonomous");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("entity="+entity);
		logger.debug("is-autonomous="+autonomous);
		
		if	(	(id			==	null)
							||
				(apikey		==	null)
							||
				(entity		==	null)
							||
				(autonomous	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if(!is_valid_owner(id))
		{
			resp.setStatusCode(403).end("id is not valid");
			return;
		}
		
		if(!is_valid_entity(entity))
		{
			resp.setStatusCode(403).end("entity is not valid");
			return;
		}
		
		if	(	!(	(autonomous.equals("true"))
							||
					(autonomous.equals("false"))
				)
			)
		{
			resp.setStatusCode(400).end("Invalid is-autonomous header");
			return;
		}
		
		if(!is_owner(id, entity))
		{
			resp.setStatusCode(403).end("You are not the owner of the entity");
			return;
		}
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			String	query	=	"UPDATE users SET is_autonomous	=	'"	+
								autonomous								+
								"' WHERE id						=	'"	+
								entity									+
								"'"										;
				
			dbService.runQuery(query, queryResult -> {
					
		if(queryResult.succeeded())
		{
			logger.debug("All ok");
			
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not update is-autonomous value");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
		
		});
	}
	
	public void owners(HttpServerRequest req)
	{
		logger.debug("In owners API");
		
		HttpServerResponse resp	=	req.response();
		String	id				=	req.getHeader("id");
		String	apikey			=	req.getHeader("apikey");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		
		if(!id.equals("admin"))
		{
			resp.setStatusCode(403).end();
			return;
		}
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			String user_query	=	"SELECT * FROM users WHERE id NOT LIKE '%/%'";
				
			dbService.runQuery(user_query, query -> {
					
		if(query.succeeded())
		{
			List<String> list	=	query.result();
						
			JsonArray response	=	new JsonArray();
						
			for(String row:list)
			{
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
							
				logger.debug("Processed row ="+Arrays.asList(processed_row));
							
				response.add(processed_row[0]);
			}
			
			logger.debug("All ok");
			
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
		else
		{
			resp.setStatusCode(500).end("Could not get owner details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	public void follow_requests	(HttpServerRequest req)
	{
		logger.debug("In follow-requests API");
		
		HttpServerResponse resp	=	req.response();
		
		String	id				=	req.getHeader("id");
		String 	apikey			=	req.getHeader("apikey");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		
		if	(	(id		==	null)
						||
				(apikey	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if	(	!	(	(is_valid_owner(id))
								^
						(is_valid_entity(id))
					)	
			)
		{
			resp.setStatusCode(403).end("Invalid id");
			return;
		}
		
		String exchange_string	=	is_valid_owner(id)?(id+"/%.%"):(is_valid_entity(id))?id+".%":"";
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			if(!login.result())
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
				
			logger.debug("Autonomous ok");
				
			String follow_query	=	"SELECT * FROM follow WHERE exchange LIKE '"	+
									exchange_string 								+
									"' AND status = 'pending' ORDER BY TIME"		;
				
			dbService.runQuery(follow_query, query -> {
					
		if(query.succeeded())
		{
			List<String>	list		=	query.result();
			JsonArray		response	=	new JsonArray();
						
			for(String row:list)
			{
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
							
				logger.debug("Processed row ="+Arrays.asList(processed_row));
							
				JsonObject temp	=	new JsonObject();
							
				temp.put("follow-id", processed_row[0]);
				temp.put("from", processed_row[1]);
				temp.put("to", processed_row[2]);
				temp.put("time", processed_row[3]);
				temp.put("permission", processed_row[4]);
				temp.put("topic", processed_row[5]);
				temp.put("validity", processed_row[6]);
							
				response.add(temp);
			}
						
			logger.debug("All ok");
						
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
		else
		{
			resp.setStatusCode(500).end("Could not get follow requests' details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	public void follow_status(HttpServerRequest req)
	{
		logger.debug("In follow-status API");
		
		HttpServerResponse resp	=	req.response();
		
		String	id				=	req.getHeader("id");
		String 	apikey			=	req.getHeader("apikey");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		
		if	(	(id		==	null)
						||
				(apikey	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if	(	!	(	(is_valid_owner(id))
								^
						(is_valid_entity(id))
					)	
			)
		{
			resp.setStatusCode(403).end("Invalid id");
			return;
		}
		
		String from_string	=	is_valid_owner(id)?(id+"/%"):(is_valid_entity(id))?id:"";
		
		check_login(id, apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			if(!login.result())
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
				
			logger.debug("Autonomous ok");
				
			String follow_query	=	"SELECT * FROM follow WHERE from_id LIKE '"	+
									from_string 								+
									"' ORDER BY TIME"	;
				
			dbService.runQuery(follow_query, query -> {
					
		if(query.succeeded())
		{
			List<String>	list		=	query.result();
			JsonArray		response	=	new JsonArray();
						
			for(String row:list)
			{
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
							
				logger.debug("Processed row ="+Arrays.asList(processed_row));
							
				JsonObject temp	=	new JsonObject();
							
				temp.put("follow-id", processed_row[0]);
				temp.put("from", processed_row[1]);
				temp.put("to", processed_row[2]);
				temp.put("time", processed_row[3]);
				temp.put("permission", processed_row[4]);
				temp.put("topic", processed_row[5]);
				temp.put("validity", processed_row[6]);
				temp.put("status", processed_row[7]);
							
				response.add(temp);
			}
						
			logger.debug("All ok");
						
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
		else
		{
			resp.setStatusCode(500).end("Could not get follow requests' details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	public void reject_follow(HttpServerRequest req)
	{
		logger.debug("In reject follow");
		
		HttpServerResponse resp	=	req.response();
		String	id				=	req.getHeader("id");
		String	apikey			=	req.getHeader("apikey");
		String	follow_id		=	req.getHeader("follow-id");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		logger.debug("follow-id="+follow_id);
		
		
		if	(	(id			==	null)
							||
				(apikey		==	null)
							||
				(follow_id	==	null)
			)
		{
			resp.setStatusCode(403).end("Inputs missing in headers");
			return;
		}
		
		if(!is_string_safe(follow_id))
		{
			resp.setStatusCode(400).end("Invalid follow-id");
			return;
		}
		
		String exchange_string	=	is_valid_owner(id)?(id+"/%.%"):(is_valid_entity(id))?id+".%":"";
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			logger.debug("Login ok");
				
			if(!login.result())
			{
				resp.setStatusCode(403).end("Unauthorised");
				return;
			}
				
			logger.debug("Autonomous ok");
				
			String follow_query	=	"SELECT * FROM follow WHERE follow_id	=	'"	+
										follow_id										+
										"' AND exchange LIKE 						'"	+
										exchange_string									+
										"' AND status = 'pending'"						;
				
			dbService.runQuery(follow_query, query -> {
					
		if(query.succeeded())
		{
			if(query.result().size()!=1)
			{
				resp.setStatusCode(400).end("Follow-id is not valid");
				return;
			}
						
			logger.debug("Follow-id is valid");
						
			String update_query	=	"UPDATE follow SET status	=	'rejected'"	+
									"WHERE follow_id			=	'"			+
									follow_id									+
									"'"											;
						
			dbService.runQuery(update_query, updateQuery -> {
							
		if(updateQuery.succeeded())
		{
			logger.debug("All ok");
								
			resp.setStatusCode(200).end();
			return;
		}
		else
		{
			resp.setStatusCode(500).end("Could not run update query on follow");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(500).end("Could not get follow details");
			return;
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
}
	
	public void permissions(HttpServerRequest req)
	{
		logger.debug("In permissions");
		
		HttpServerResponse	resp	=	req.response();
		
		String	id					=	req.getHeader("id");
		String	apikey				=	req.getHeader("apikey");
		String	entity				=	req.getHeader("entity");
		
		logger.debug("id="+id);
		logger.debug("apikey="+apikey);
		
		if	(	(id		==	null)
						||
				(apikey	==	null)
			)
		{
			resp.setStatusCode(400).end("Inputs missing in headers");
			return;
		}
		
		if	(	!	(	(is_valid_owner(id))
								^
						(is_valid_entity(id))
					)	
			)
		{
			resp.setStatusCode(403).end("Invalid id");
			return;
		}
		
		if(is_valid_owner(id))
		{
			if(entity	==	null)
			{
				resp.setStatusCode(400).end("Entity value not specified in headers");
				return;
			}
			
			if(!is_owner(id, entity))
			{
				resp.setStatusCode(403).end("You are not the owner of the entity");
				return;
			}
		}
		
		String from_id	=	(is_valid_owner(id))?entity:(is_valid_entity(id)?id:"");
		
		check_login(id,apikey)
		.setHandler(login -> {
			
		if(login.succeeded())
		{
			String 	acl_query	=	"SELECT * FROM acl WHERE from_id	=	'"	+
									from_id										+
									"' AND valid_till > now()"					;
				
			dbService.runQuery(acl_query, query -> {
					
		if(query.succeeded())
		{
			List<String>	list		=	query.result();
			JsonArray 		response	=	new JsonArray();
						
			for(String row:list)
			{
				String	processed_row[]	=	row.substring(row.indexOf("[")+1, row.indexOf("]")).trim().split(",\\s");
							
				logger.debug("Processed row ="+Arrays.asList(processed_row));
										
				JsonObject temp	=	new JsonObject();
							
				temp.put("entity", processed_row[1]);
				temp.put("permission", processed_row[2]);
							
				response.add(temp);
			}
					
			logger.debug("All ok");
						
			resp
			.putHeader("content-type", "application/json")
			.setStatusCode(200)
			.end(response.encodePrettily());
		}
	});
		}
		else
		{
			resp.setStatusCode(403).end("Invalid id or apikey");
			return;
		}
	});
		
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
		
		//TODO: Add proper validation
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
	 * subscription request by clients.apikey
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
		create_broker_client	=	Future.future();
		broker_config			=	new RabbitMQOptions();
		
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
		logger.debug("In entity does not exist");
		
		Future<Void> future	=	Future.future();		
		String query		=	"SELECT * FROM users WHERE id = '"
								+ registration_entity_id +	"'";
		
		dbService.runQuery(query, reply -> {
			
		if(reply.succeeded())
		{	
			List<String> resultList	=	reply.result();
			int rowCount			=	resultList.size();
			
			if(rowCount>0)
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
		logger.debug("in entity does not exist");
		
		Future<Void> future		=	Future.future();		
		String query			=	"SELECT * FROM users WHERE id = '"
									+ registration_entity_id +	"'";
		
		dbService.runQuery(query, reply -> {
			
		if(reply.succeeded())
		{
			List<String> resultList	=	reply.result();
			int rowCount			=	resultList.size();
		
			if(rowCount>0)
			{
				future.fail("Entity exists");	
			}
			else
			{
				future.complete();
			}
				
		}
		else
		{
			logger.debug(reply.cause());
			future.fail("Could not get entity details");
		}
	});
		return future;
	}
	
	private boolean is_owner(String owner, String entity)
	{
		logger.debug("In is_owner");
		
		logger.debug("Owner="+owner);
		logger.debug("Entity="+entity);
		
		if	(	(entity.startsWith(owner))
						&&
				(entity.contains("/"))
			)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	public Future<String> generate_credentials(String id, String schema, String autonomous) 
	{
		logger.debug("In generate credentials");
		
		Future<String> future	= Future.future();
		
		String apikey			=	genRandString(32);
		String salt 			=	genRandString(32);
		String blocked 			=	"f";
		
		String string_to_hash	=	apikey + salt + id;
		String hash				=	Hashing.sha256()
									.hashString(string_to_hash, StandardCharsets.UTF_8)
									.toString();
		
		logger.debug("Id="+id);
		logger.debug("Generated apikey="+apikey);
		logger.debug("Salt="+salt);
		logger.debug("String to hash="+string_to_hash);
		logger.debug("Hash="+hash);
		
		String query			=	"INSERT INTO users VALUES('"
									+id			+	"','"
									+hash		+	"','"
									+schema 	+	"','"
									+salt		+ 	"','"
									+blocked	+	"','"
									+autonomous +	"')";
		
		dbService.runQuery(query, reply -> {
			
		if(reply.succeeded())
		{
			logger.debug("Generate credentials query succeeded");
			future.complete(apikey);
		}
		else
		{
			logger.debug("Failed to run query. Cause="+reply.cause());
			future.fail(reply.cause().toString());
		}
		
	});
		
		return future;
	}
	
	public Future<String> update_credentials(String id)
	{
		logger.debug("In update credentials");
		
		Future<String> future	=	Future.future();

		String apikey			=	genRandString(32);
		String salt 			=	genRandString(32);
		
		String string_to_hash	=	apikey + salt + id;
		String hash				=	Hashing.sha256()
									.hashString(string_to_hash, StandardCharsets.UTF_8)
									.toString();
		
		logger.debug("Id="+id);
		logger.debug("Generated apikey="+apikey);
		logger.debug("Salt="+salt);
		logger.debug("String to hash="+string_to_hash);
		logger.debug("Hash="+hash);
		
		String update_query		=	"UPDATE users SET password_hash	=	'" 
									+	hash	+	"',	salt	=		'" 
									+ 	salt	+	"' WHERE id	=		'"
									+	id		+	"'"					;														
		
		dbService.runQuery(update_query, reply -> {
			
		if(reply.succeeded())
		{
			logger.debug("Update credentials query succeeded");
			future.complete(apikey);
		}
		else
		{
			logger.debug("Failed to run query. Cause="+reply.cause());
			future.fail(reply.cause().toString());
		}
	});
		
		return future;
}
	
	public String genRandString(int len)
	{
		logger.debug("In genRandString");
		
		String randStr	=	RandomStringUtils
							.random(len, 0, PASSWORDCHARS.length(), 
							true, true, PASSWORDCHARS.toCharArray());
		
		logger.debug("Generated random string = "+randStr);
		
		return randStr;
	}
	
	public Future<Boolean> check_login(String id, String apikey)
	{
		logger.debug("In check_login");
		
		logger.debug("ID="+id);
		logger.debug("Apikey="+apikey);
		
		Future<Boolean> check = Future.future();

		if(id.equalsIgnoreCase("") || apikey.equalsIgnoreCase(""))
		{
			check.fail("Invalid credentials");
		}
		
		if(!(is_valid_owner(id) ^ is_valid_entity(id)))
		{
			check.fail("Invalid credentials");
		}
		
		String query		=	"SELECT * FROM users WHERE id	=	'"	+
								id		+ 							"'"	+
								"AND blocked = 'f'"						;
		
		dbService.runQuery(query, reply -> {
			
		if(reply.succeeded())
		{	
			List<String> resultList	=	reply.result();
			int rowCount			=	resultList.size();
			
			if(rowCount==0)
			{
				check.fail("Not found");
			}
			
			else if(rowCount==1)
			{
				String raw				=	resultList.get(0);
				String row[] 			=	raw
											.substring(	raw.indexOf("[")+1, 
														raw.indexOf("]"))
											.split(",\\s");

				String salt 			=	row[3];
				String string_to_hash	=	apikey	+	salt	+	id;
				String expected_hash 	= 	row[1];
				String actual_hash 		= 	Hashing
											.sha256()
											.hashString(string_to_hash, StandardCharsets.UTF_8)
											.toString();
				
				boolean autonomous		=	row[5].equals("true")?true:false;
				
				logger.debug("Salt ="+salt);
				logger.debug("String to hash="+string_to_hash);
				logger.debug("Expected hash ="+expected_hash);
				logger.debug("Actual hash ="+actual_hash);
										
				if(actual_hash.equals(expected_hash))
				{
					check.complete(autonomous);
				}
				else
				{
					check.fail("Invalid credentials");
				}
			}
			else
			{
				check.fail("Something is terribly wrong");
			}
		}
	});
		
	return check;
	}

	public boolean is_string_safe(String resource)
	{
		logger.debug("In is_string_safe");
		
		boolean safe = (resource.length() - (resource.replaceAll("[^#-/a-zA-Z0-9]+", "")).length())==0?true:false;
		
		logger.debug("Original resource name ="+resource);
		logger.debug("Replaced resource name ="+resource.replaceAll("[^#-/a-zA-Z0-9]+", ""));
		return safe;
	}
	
	public boolean is_valid_owner(String owner_name)
	{
		logger.debug("In is_valid_owner");
		
		//TODO simplify this
		if	(	(!Character.isDigit(owner_name.charAt(0)))
									&&
			(	(owner_name.length() - (owner_name.replaceAll("[^a-z0-9]+", "")).length())==0)
			)
		{
			logger.debug("Original owner name = "+owner_name);
			logger.debug("Replaced name = "+owner_name.replaceAll("[^a-z0-9]+", ""));
			return true;
		}
		else
		{
			logger.debug("Original owner name = "+owner_name);
			logger.debug("Replaced name = "+owner_name.replaceAll("[^a-z0-9]+", ""));
			return false;
		}
	}
	
	public boolean is_valid_entity(String resource)
	{
		logger.debug("In is_valid_entity");
		
		String entries[]	=	resource.split("/");
		
		logger.debug("Entries = "+Arrays.asList(entries));
		
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

package main.java.iudx.apiserver;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.reactiverse.pgclient.PgConnection;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class DbClient extends AbstractVerticle 
{
	public PgPool client;

	@Override
	public void start(Future<Void> startFuture) throws Exception
	{
		PgPoolOptions options = new PgPoolOptions();
        options.setDatabase(URLs.psql_database_name);
        options.setHost(URLs.psql_database_url); 
        options.setPort(URLs.psql_database_port);
        options.setUser(URLs.psql_database_username);
        options.setPassword(URLs.psql_database_password);
        options.setCachePreparedStatements(true);
        options.setMaxSize(10);

		client = PgClient.pool(vertx, options);

		vertx.eventBus().consumer("db.queue", this::onMessage);
		startFuture.complete();
	}

	public void onMessage(Message<JsonObject> message)
	{
		if(!message.headers().contains("action"))
		{
			return;
		}

		String action = message.headers().get("action");

		switch(action)
		{
			case "select-query":
				selectQuery(message);
				break;
			case "insert-query":
			case "update-query":
			case "delete-query":
				runQuery(message);
				break;
		}
	}
	
	public void runQuery(Message<JsonObject> message)
	{
		JsonObject reply = new JsonObject();
		
		client.getConnection(ar -> {
			
			if(ar.succeeded())
			{
				PgConnection conn 	=  ar.result();
				
				String query 		= message.body().getString("query");
				
				System.out.println("Query = " + query);
				
				conn.preparedQuery(query, result -> {
					
					if(result.succeeded())
					{
						System.out.println("query succeeded");
						reply.put("status", "success");
					}
					else
					{
						System.out.println("query failed");
						reply.put("status", "error");
					}
					
					message.reply(reply);
				});
			}
		});
	}

	public void selectQuery(Message<JsonObject> message)
	{
		JsonObject reply = new JsonObject();
		JsonObject queryResult = new JsonObject();
		
		client.getConnection( ar -> {

			if(ar.succeeded())
			{
				PgConnection conn 	=  ar.result();

				String query 		= message.body().getString("query");
				String columns[]	= message.body().getString("columns").split(",");

				conn.query(query, res -> {

				if(res.succeeded())
				{	
					PgRowSet rows = res.result();
					int rowSize = rows.rowCount();
						
					reply.put("rowCount",rowSize);
					reply.put("status", "success");
						
					if(rowSize!=0)
					{
						if(rowSize==1)
						{
							Row row = rows.iterator().next();
							
							for(String columnName:columns)
							{	
								queryResult.put(columnName, row.getString(columnName));
							}
						}
						else // executed only when querying for owner entities
						{
							//TODO: Use StringBuilder
							String list ="";
							
							//TODO: Unsafe. May block the event-loop
							for(Row row:rows)
							{
								list = list + row.getString(columns[0]) + ",";
							}
							
							queryResult.put(columns[0], list);
						}
					
						reply.put("result", queryResult);
					}
			
				}
				else
				{
					reply.put("status", res.cause().toString());
				}
				conn.close();
					
				message.reply(reply);
				});
			}
		});
	}
}
package database;

import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgRowSet;
import io.reactiverse.pgclient.Row;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class DbServiceImpl implements DbService
{
	PgPoolOptions options;
	PgPool client;
	Vertx vertx;
	
	public DbServiceImpl(Vertx vertx, PgPoolOptions options) 
	{
		this.vertx		=	vertx;
		this.options	=	options;
		client			=	PgClient.pool(vertx, options);
	}

	@Override
	public DbService runQuery(String query, Handler<AsyncResult<Void>> resultHandler) 
	{
		client.getConnection(ar -> {
			
			if(ar.succeeded())
			{
				PgConnection conn 	=  ar.result();
				
				System.out.println("Query = " + query);
				
				conn.preparedQuery(query, result -> {
					
					if(result.succeeded())
					{
						resultHandler.handle(Future.succeededFuture());
					}
					else
					{
						resultHandler.handle(Future.failedFuture(result.cause()));
					}
				});
			}
		});
		
		return this;
	}

	@Override
	public DbService selectQuery(String query, String column_list, Handler<AsyncResult<JsonObject>> resultHandler) 
	{
		JsonObject reply = new JsonObject();
		JsonObject queryResult = new JsonObject();
		
		client.getConnection( ar -> {

			if(ar.succeeded())
			{
				PgConnection conn 	=  ar.result();
				String columns[]	= column_list.split(",");

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
				
				resultHandler.handle(Future.succeededFuture(reply));
				
				});
			}
		});
		
		return this;
	}

}

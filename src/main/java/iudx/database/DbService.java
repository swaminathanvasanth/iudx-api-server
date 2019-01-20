package iudx.database;

import io.reactiverse.pgclient.PgPoolOptions;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
@VertxGen
public interface DbService 
{
	@Fluent
	DbService runQuery(String query, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	DbService selectQuery(String query, String column_list, Handler<AsyncResult<JsonObject>> resultHandler);
	
	@GenIgnore 
	static DbService create(Vertx vertx, PgPoolOptions options, Handler<AsyncResult<DbService>> resultHandler)
	{
		return new DbServiceImpl(vertx, options, resultHandler);
	}
	
	@GenIgnore
	static DbService createProxy(Vertx vertx, String address)
	{
		return new DbServiceVertxEBProxy(vertx, address);
	}
}

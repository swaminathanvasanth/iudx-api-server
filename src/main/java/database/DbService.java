package database;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

@ProxyGen
@VertxGen
public interface DbService 
{
	@Fluent
	DbService runQuery(String query, Handler<AsyncResult<Void>> resultHandler);
	
	@Fluent
	DbService selectQuery(String query, String column_list, Handler<AsyncResult<JsonObject>> resultHandler);
}

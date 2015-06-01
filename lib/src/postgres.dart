library postgresql_warehouse.postgres;

import 'dart:async';
import 'package:postgresql/pool.dart';
import 'package:warehouse/adapters/sql.dart';
import 'package:warehouse/src/adapters/sql/where_clause.dart';
import 'package:warehouse/sql.dart';
import 'package:postgresql_warehouse/src/handle_result.dart';
import 'package:postgresql_warehouse/src/sql.dart';

export 'package:postgresql/pool.dart' show Pool;

class Postgres extends SqlDbBase with PostgresEndpoint {
  final Pool pool;
  final Map<Type, MatchVisitor> matchers = {
    RegexpMatcher: (RegexpMatcher matcher, List parameters, LookingGlass lg) {
      setParameter(parameters, matcher.regexp, lg);
      return '{field} ~* ?';
    },
  };

  Postgres(this.pool);

  @override
  Future sql(String sql, {List parameters, bool returnCreated: false}) async {
    if (pool.state != PoolState.running) {
      await pool.start();
    }
    var connection = await pool.connect();
    try {
      if (returnCreated) {
        sql += ' RETURNING id';
      }
      var result =
          await connection.query(prepareSql(sql, parameters), parameters);
      if (returnCreated) {
        return (await result.first).id;
      }
      return handleResult(result);
    } finally {
      connection.close();
    }
  }

  @override
  Future createTables() async {
    for (var table in tables) {
      var query = new StringBuffer('CREATE TABLE IF NOT EXISTS ');
      query.write(escapeChar);
      query.write(table.name);
      query.write(escapeChar);
      query.write('(id SERIAL PRIMARY KEY');
      table.columns.forEach((name, dataType) {
        query.write(', "');
        query.write(name);
        query.write('" ');
        query.write(dataType);
      });
      query.write(') WITH OIDS;');

      await sql(query.toString());
    }
  }

  @override
  Future<SqlTransaction> startTransaction() async {
    if (pool.state != PoolState.running) {
      await pool.start();
    }
    var connection = await pool.connect();
    try {
      await connection.query('START TRANSACTION');
      return new PostgresTransaction(connection);
    } catch (_) {
      connection.close();
    }
  }
}

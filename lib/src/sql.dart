library postgresql_warehouse.sql;

import 'dart:async';
import 'package:postgresql/postgresql.dart';
import 'package:warehouse/adapters/sql.dart';
import 'package:warehouse/src/adapters/sql/where_clause.dart';
import 'package:warehouse/sql.dart';
import 'package:postgresql_warehouse/src/handle_result.dart';

String prepareSql(String sql, List parameters) {
  var index = -1;
  if (parameters != null) {
    sql = sql.replaceAll('@', '@@');
  }
  return sql.replaceAllMapped('?', (_) {
    index += 1;
    return '@$index';
  });
}

class PostgresSelectQuery extends SelectQueryImplementation {
  PostgresSelectQuery(SqlEndpoint db, List<String> projections)
      : super(db, projections);

  @override
  SelectQuery join(Map<String, String> fieldTable) {
    var projections = fieldTable.keys.map((field) => "'$field'");
    if (projections_.isEmpty) {
      projections_ = projections.toList()..add('*');
    } else {
      projections_ = projections_..addAll(projections);
    }
    return super.join(fieldTable);
  }
}

abstract class PostgresEndpoint implements SqlEndpoint {
  final String escapeChar = '"';
  final Map<Type, String> dataTypes = const {
    int: 'INTEGER',
    num: 'FLOAT8',
    String: 'VARCHAR(255)',
    DateTime: 'BIGINT',
    GeoPoint: 'VARCHAR(255)',
    Type: 'VARCHAR(255)',
  };

  SelectQuery select([List<String> projections = const []]) =>
      new PostgresSelectQuery(this, projections);
}

class PostgresTransaction extends SqlTransaction with PostgresEndpoint {
  final Connection connection;
  final Map<Type, MatchVisitor> matchers = {
    RegexpMatcher: (RegexpMatcher matcher, List parameters, LookingGlass lg) {
      setParameter(parameters, matcher.regexp, lg);
      return '{field} ~* ?';
    },
  };

  PostgresTransaction(this.connection);

  @override
  Future commit() async {
    try {
      await connection.execute('COMMIT');
    } finally {
      connection.close();
    }
  }

  @override
  Future rollback() async {
    try {
      await connection.execute('ROLLBACK');
    } finally {
      connection.close();
    }
  }

  @override
  Future sql(String sql, {List parameters, bool returnCreated: false}) async {
    if (returnCreated) {
      sql += ' RETURNING id';
    }
    Stream<Row> result =
    await connection.query(prepareSql(sql, parameters), parameters);
    if (returnCreated) {
      var id = (await result.first).id;
      return id;
    }
    return handleResult(result);
  }
}

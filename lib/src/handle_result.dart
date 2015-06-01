library postgresql_warehouse.handle_request;

import 'dart:async';
import 'dart:collection';
import 'package:postgresql/postgresql.dart';

handleResult(Stream<Row> rows) async {
  List<Column> columns;
  var fieldOffset;

  var documents = {};
  await for (Row row in rows) {
    if (columns == null) {
      columns = row.toColumns();
      fieldOffset =
          columns.indexOf(columns.firstWhere((column) => column.fieldId != 0));
    }

    var document = new HashMap();
    var join = new HashMap();
    var mainTable = true;
    var tableCount = -1;
    var tableName;

    for (var i = fieldOffset; i < columns.length; i++) {
      var field = columns[i];

      if (field.name == 'id') {
        if (tableCount >= 0) {
          mainTable = false;
          tableName = row[tableCount];
        }

        tableCount++;
      }

      if (mainTable) {
        document[field.name] = row[i];
      } else {
        if (row[i] == null) continue;

        if (!join.containsKey(tableName)) {
          join[tableName] = new HashMap();
        }
        join[tableName][field.name] = row[i];
      }
    }

    var id = row[fieldOffset];

    if (documents.containsKey(id)) {
      join.forEach((table, columns) {
        if (documents[id][table] is! List) {
          documents[id][table] = [documents[id][table]];
        }
        documents[id][table].add(columns);
      });
    } else {
      join.forEach((table, columns) {
        document[table] = columns;
      });

      documents[id] = document;
    }
  }
  return documents.values;
}

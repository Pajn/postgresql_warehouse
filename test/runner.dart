import 'package:unittest/unittest.dart';
import 'package:postgresql/postgresql.dart';
import 'package:warehouse/adapters/conformance_tests.dart';
import 'package:warehouse/sql.dart';
import 'package:postgresql_warehouse/postgresql_warehouse.dart';

Postgres db;

class TestConfiguration extends SimpleConfiguration {
  onTestResult(TestCase result) {
    print(formatResult(result).trim());
  }

  void onSummary(int passed, int failed, int errors, List<TestCase> results,
                 String uncaughtError) {
    // Show the summary.
    print('');

    if (passed == 0 && failed == 0 && errors == 0 && uncaughtError == null) {
      print('No tests found.');
      // This is considered a failure too.
    } else if (failed == 0 && errors == 0 && uncaughtError == null) {
      print('All $passed tests passed.');
    } else {
      if (uncaughtError != null) {
        print('Top-level uncaught error: $uncaughtError');
      }
      print('$passed PASSED, $failed FAILED, $errors ERRORS');
    }

    db.pool.stop();
  }
}

main() async {
  unittestConfiguration = new TestConfiguration();

  var uri = 'postgres://postgres:pass@localhost:5432/';
  var connection = await connect(uri);
  await connection.execute('DROP DATABASE IF EXISTS warehouse_postgres_test');
  await connection.execute('CREATE DATABASE warehouse_postgres_test');
  connection.close();

  uri = 'postgres://postgres:pass@localhost:5432/warehouse_postgres_test';
  db = new Postgres(new Pool(uri));

  await registerModels(db);
  runConformanceTests(
          () => new SqlDbSession(db),
          (session, type) => new SqlRepository.withTypes(session, [type])
  );
}

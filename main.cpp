#include <stdlib.h>
#include <string>
#include <iostream>

// include the sql parser
#include "/usr/local/include/hsql/SQLParser.h"
// contains printing utilities
#include "/usr/local/include/hsql/util/sqlhelper.h"

#include "src/include/common/bustub_instance.h"
#include "Executor.h"

Executor * executor;
void action (hsql::SQLParserResult & result) {
  hsql::SQLStatement const * stmt = result.getStatement(0);
  if (stmt->isType(hsql::kStmtSelect)) {
    hsql::SelectStatement const* select = static_cast<const hsql::SelectStatement*>(stmt);
    executor->ExecuteSelect(select);
  } else if (stmt->isType(hsql::kStmtInsert)) {
    hsql::InsertStatement const* insert = static_cast<const hsql::InsertStatement*>(stmt);
    executor->ExecuteInsert(insert);
  } else {
    std::cout << "Unsupported\n";
  }
}

void REPL () {
  std::string query;
  while (true) {
    std::cout << ">> ";
    std::getline (std::cin, query);
    if (query == "exit") {
      return;
    }
    hsql::SQLParserResult result;
    hsql::SQLParser::parse(query, &result);

    if (result.isValid()) {
      // result.getStatements();
      action (result);
    } else {
      fprintf(stderr, "Given string is not a valid SQL query.\n");
      fprintf(stderr, "%s (L%d:%d)\n",
      result.errorMsg(),
      result.errorLine(),
      result.errorColumn());
    }
  }
}

void setup () {
  executor = new Executor();
  executor->SetUp();
}

int main(int argc, char* argv[]) {
  setup();
  REPL();

  delete executor;
  return 0;
}

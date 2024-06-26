# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' renderTranslateExecuteSql
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param errorReportFile error file
#' @param snakeCaseToCamelCase snake case to camel case boolean
#' @param oracleTempSchema oracle temp schema
#' @param tempEmulationSchema temp emulation schema
#' @param integerAsNumeric int as numeric boolean
#' @param integer64AsNumeric int64 as numeric boolean
#' @param reportOverallTime ignored
#' @param progressBar ignored
#' @param ... parameters that will be used to render the SQL.
#' 
#' @return NONE
renderTranslateExecuteSql <- function(connection,
                                      sql,
                                      errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                      snakeCaseToCamelCase = FALSE,
                                      oracleTempSchema = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                      integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                      reportOverallTime = FALSE,
                                      progressBar = FALSE,
                                      ...) {
  
  sql <- SqlRender::render(sql = sql, ...)
  sql <- SqlRender::translate(sql = sql, targetDialect = getDbms(connection))

  dbms <- getDbms(connection)
  if (dbms == "sqlite") {
    DBI::dbExecute(conn = connection, statement = sql)
  } else {
    purrr::walk(SqlRender::splitSql(sql), ~DBI::dbExecute(conn = connection, statement = .))
  }
}

#' renderTranslateQuerySql
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param errorReportFile error file
#' @param snakeCaseToCamelCase snake case to camel case boolean
#' @param oracleTempSchema oracle temp schema
#' @param tempEmulationSchema temp emulation schema
#' @param integerAsNumeric int as numeric boolean
#' @param integer64AsNumeric int64 as numeric boolean
#' @param ... parameters that will be used to render the SQL.
#'
#' @return the query results
renderTranslateQuerySql <- function(connection,
                                    sql,
                                    errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                    snakeCaseToCamelCase = FALSE,
                                    oracleTempSchema = NULL,
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                                    integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE),
                                    ...) {
  sql <- SqlRender::render(sql = sql, ...)
  sql <- SqlRender::translate(sql = sql, targetDialect = getDbms(connection))
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

#' querySql
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param errorReportFile error file
#' @param snakeCaseToCamelCase snake case to camel case boolean
#' @param integerAsNumeric int as numeric boolean
#' @param integer64AsNumeric int64 as numeric boolean
#'
#' @return query results
#'
querySql <- function(connection,
                     sql,
                     errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                     snakeCaseToCamelCase = FALSE,
                     integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric", default = TRUE),
                     integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric", default = TRUE)) {
  result <- DBI::dbGetQuery(conn = connection, statement = sql)
  if (snakeCaseToCamelCase) {
    colnames(result) <- SqlRender::snakeCaseToCamelCase(colnames(result))
  }
  return(result)
}

#' querySqlToAndromeda
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param andromeda andromeda object
#' @param andromedaTableName table name
#' @param errorReportFile error file
#' @param snakeCaseToCamelCase snake case to camel case boolean
#' @param appendToTable if result should be appended
#' @param integerAsNumeric int as numeric boolean
#' @param integer64AsNumeric int64 as numeric boolean
#'
#' @return the updated andromeda object
querySqlToAndromeda <- function(connection,
                                sql,
                                andromeda,
                                andromedaTableName,
                                errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                snakeCaseToCamelCase = FALSE,
                                appendToTable = FALSE,
                                integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                             default = TRUE
                                ),
                                integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                               default = TRUE
                                )) {
  if (isCDMConnection(connection)) {
    andromeda[[andromedaTableName]] <- querySql(connection = connection,
                                                sql = sql,
                                                snakeCaseToCamelCase = snakeCaseToCamelCase)
  } else {
    DatabaseConnector::querySqlToAndromeda(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      andromeda = andromeda,
      andromedaTableName = andromedaTableName
    )
  }
  return(andromeda)
}

#' renderTranslateQuerySqlToAndromeda
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param andromeda andromeda object
#' @param andromedaTableName table name
#' @param errorReportFile error file
#' @param snakeCaseToCamelCase snake case to camel case boolean
#' @param appendToTable if result should be appended
#' @param integerAsNumeric int as numeric boolean
#' @param integer64AsNumeric int64 as numeric boolean
#' @param ... parameters that will be used to render the SQL
#'
#' @return the updated andromeda object
renderTranslateQuerySqlToAndromeda <- function(connection,
                                               sql,
                                               andromeda,
                                               andromedaTableName,
                                               errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                                               snakeCaseToCamelCase = FALSE,
                                               appendToTable = FALSE,
                                               integerAsNumeric = getOption("databaseConnectorIntegerAsNumeric",
                                                                             default = TRUE),
                                               integer64AsNumeric = getOption("databaseConnectorInteger64AsNumeric",
                                                                               default = TRUE),
                                               ...) {
  sql <- SqlRender::render(sql = sql, ...)
  sql <- SqlRender::translate(sql = sql, targetDialect = getDbms(connection))
  querySqlToAndromeda(connection = connection,
                      sql = sql,
                      andromeda = andromeda,
                      andromedaTableName = andromedaTableName,
                      errorReportFile = errorReportFile,
                      snakeCaseToCamelCase = snakeCaseToCamelCase,
                      appendToTable = appendToTable,
                      integerAsNumeric = integerAsNumeric,
                      integer64AsNumeric = integer64AsNumeric)
}

#' executeSql
#'
#' @param connection db connection
#' @param sql The SQL to be executed
#' @param profile profile
#' @param progressBar progress bar
#' @param reportOverallTime if overall time should be reported
#' @param errorReportFile error report file
#' @param runAsBatch run as batch?
#'
executeSql <- function(connection,
                       sql,
                       profile = FALSE,
                       progressBar = !as.logical(Sys.getenv("TESTTHAT", unset = FALSE)),
                       reportOverallTime = TRUE,
                       errorReportFile = file.path(getwd(), "errorReportSql.txt"),
                       runAsBatch = FALSE) {
  # execute
  dbms <- getDbms(connection)
  if (dbms == "sqlite") {
    DBI::dbExecute(conn = connection, statement = sql)
  } else {
    purrr::walk(SqlRender::splitSql(sql), ~DBI::dbExecute(conn = connection, statement = .))
  }
}


#' isCDMConnection
#'
#' @param connection db connection
#'
#' @return if this is CDM connection
#' 
isCDMConnection <- function(connection) {
  return(!("dbms" %in% names(attributes(connection))))
}

#' getDbms
#'
#' @param connection db connection
#'
#' @return dbms
#' 
getDbms <- function(connection) {
  result <- NULL
  if (isCDMConnection(connection)) {
    result <- CDMConnector::dbms(connection)
  } else {
    result <- connection@dbms
  }
  return(result)
}

#' getTableNames
#'
#' @param connection db connection
#' @param cdmDatabaseSchema schema name
#'
#' @return the table names
#' 
getTableNames <- function(connection, cdmDatabaseSchema) {
  result <- NULL
  if (isCDMConnection(connection)) {
    result <- CDMConnector::listTables(connection, cdmDatabaseSchema)
  } else {
    result <- DatabaseConnector::getTableNames(connection, cdmDatabaseSchema)
  }
  return(result)
}

#' existsTable
#'
#' @param connection db connection
#' @param databaseSchema db schema name
#' @param tableName the table name
#'
#' @return if given table exists
existsTable <- function(connection, databaseSchema, tableName) {
  tableName %in% getTableNames(connection, databaseSchema)
}

#' Insert a table on the server
#'
#' @description
#' This function sends the data in a data frame to a table on the server. Either a new table
#' is created, or the data is appended to an existing table.
#'
#' @param connection          DB connection
#' @param databaseSchema      db schema
#' @param tableName           The name of the table where the data should be inserted.
#' @param data                The data frame containing the data to be inserted.
#' @param dropTableIfExists   Drop the table if the table already exists before writing?
#' @param createTable         Create a new table? If false, will append to existing table.
#' @param tempTable           Should the table created as a temp table?
#' @param oracleTempSchema    oracle temp schema name
#' @template TempEmulationSchema 
#' @param bulkLoad            If using Redshift, PDW, Hive or Postgres, use more performant bulk loading
#'                            techniques. Does not work for temp tables (except for HIVE). See Details for
#'                            requirements for the various platforms.
#' @param useMppBulkLoad      DEPRECATED. Use `bulkLoad` instead.
#' @param progressBar         Show a progress bar when uploading?
#' @param camelCaseToSnakeCase If TRUE, the data frame column names are assumed to use camelCase and
#'                             are converted to snake_case before uploading.
#'
#' @details
#' This function sends the data in a data frame to a table on the server. Either a new table is
#' created, or the data is appended to an existing table. NA values are inserted as null values in the
#' database.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "mysql",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah"
#' )
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(conn, "my_schema", "my_table", data)
#' disconnect(conn)
#'
#' ## bulk data insert with Redshift or PDW
#' connectionDetails <- createConnectionDetails(
#'   dbms = "redshift",
#'   server = "localhost",
#'   user = "root",
#'   password = "blah",
#'   schema = "cdm_v5"
#' )
#' conn <- connect(connectionDetails)
#' data <- data.frame(x = c(1, 2, 3), y = c("a", "b", "c"))
#' insertTable(
#'   connection = connection,
#'   databaseSchema = "scratch",
#'   tableName = "somedata",
#'   data = data,
#'   dropTableIfExists = TRUE,
#'   createTable = TRUE,
#'   tempTable = FALSE,
#'   bulkLoad = TRUE
#' ) # or, Sys.setenv("DATABASE_CONNECTOR_BULK_UPLOAD" = TRUE)
#' }
insertTable <- function(connection,
                        databaseSchema = NULL,
                        tableName,
                        data,
                        dropTableIfExists = TRUE,
                        createTable = TRUE,
                        tempTable = FALSE,
                        oracleTempSchema = NULL,
                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                        bulkLoad = Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD"),
                        useMppBulkLoad = Sys.getenv("USE_MPP_BULK_LOAD"),
                        progressBar = FALSE,
                        camelCaseToSnakeCase = FALSE) {
  
  if (isCDMConnection(connection)) {
    if (camelCaseToSnakeCase) {
      colnames(data) <- SqlRender::camelCaseToSnakeCase(colnames(data))
    }
    DBI::dbWriteTable(connection, DBI::Id(schema = databaseSchema, table = tableName), data, overwrite = TRUE) # temp table gives issues on duckDB: SqlRender just removes "#"?
  } else {
    DatabaseConnector::insertTable(connection,
                                   databaseSchema,
                                   tableName,
                                   data,
                                   dropTableIfExists,
                                   createTable,
                                   tempTable,
                                   oracleTempSchema,
                                   tempEmulationSchema,
                                   bulkLoad,
                                   useMppBulkLoad,
                                   progressBar,
                                   camelCaseToSnakeCase)
  }
}

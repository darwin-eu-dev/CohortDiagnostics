# fix for linux systems with weird rJava behaviour
if (is.null(getOption("java.parameters"))) {
  options(java.parameters = "-Xss100m")
}

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0) {
    return(y)
  }
  x
}

getConfigValue <- function(values, key, default = NULL) {
  value <- values[[key]]
  if (is.null(value)) {
    return(default)
  }
  value
}

buildConnectionDetailsFromEnvironment <- function(shinySettings, envConfig) {
  defaultConnection <- shinySettings$connectionDetails

  envNames <- list(
    dbms = getConfigValue(envConfig, "dbms", ""),
    server = getConfigValue(envConfig, "server", ""),
    database = getConfigValue(envConfig, "database", ""),
    user = getConfigValue(envConfig, "user", ""),
    password = getConfigValue(envConfig, "password", ""),
    port = getConfigValue(envConfig, "port", ""),
    extraSettings = getConfigValue(envConfig, "extraSettings", "")
  )

  serverValue <- Sys.getenv(envNames$server, unset = defaultConnection$server %||% "")
  databaseValue <- Sys.getenv(envNames$database, unset = "")

  if (nzchar(databaseValue)) {
    serverValue <- paste0(serverValue, "/", databaseValue)
  }

  list(
    dbms = Sys.getenv(envNames$dbms, unset = defaultConnection$dbms %||% ""),
    server = serverValue,
    user = Sys.getenv(envNames$user, unset = defaultConnection$user %||% ""),
    password = Sys.getenv(envNames$password, unset = defaultConnection$password %||% ""),
    port = Sys.getenv(envNames$port, unset = defaultConnection$port %||% ""),
    extraSettings = Sys.getenv(envNames$extraSettings, unset = defaultConnection$extraSettings %||% "")
  )
}

loadShinySettings <- function(configPath) {
  stopifnot(file.exists(configPath))
  shinySettings <- yaml::read_yaml(configPath)

  defaults <- list(
    resultsDatabaseSchema = "main",
    vocabularyDatabaseSchemas = "main",
    tablePrefix = "",
    cohortTableName = "cohort",
    databaseTableName = "database",
    connectionDetails = list(dbms = "sqlite", server = "data/MergedCohortDiagnosticsData.sqlite"),
    connectionEnvironmentVariables = NULL
  )

  for (name in names(defaults)) {
    shinySettings[[name]] <- shinySettings[[name]] %||% defaults[[name]]
  }

  if (is.null(shinySettings$vocabularyDatabaseSchemas)) {
    shinySettings$vocabularyDatabaseSchemas <- shinySettings$resultsDatabaseSchema
  }

  if (identical(shinySettings$cohortTableName, "cohort")) {
    shinySettings$cohortTableName <- paste0(shinySettings$tablePrefix, shinySettings$cohortTableName)
  }

  if (identical(shinySettings$databaseTableName, "database")) {
    shinySettings$databaseTableName <- paste0(shinySettings$tablePrefix, shinySettings$databaseTableName)
  }

  if (!is.null(shinySettings$connectionDetailsSecureKey)) {
    if (!requireNamespace("keyring", quietly = TRUE)) {
      stop("`keyring` package is required when `connectionDetailsSecureKey` is configured.")
    }
    shinySettings$connectionDetails <- jsonlite::fromJSON(keyring::key_get(shinySettings$connectionDetailsSecureKey))
  } else if (!is.null(shinySettings$connectionEnvironmentVariables$server)) {
    shinySettings$connectionDetails <- buildConnectionDetailsFromEnvironment(
      shinySettings = shinySettings,
      envConfig = shinySettings$connectionEnvironmentVariables
    )
  }

  shinySettings$connectionDetails <- do.call(DatabaseConnector::createConnectionDetails, shinySettings$connectionDetails)
  shinySettings
}

if (!exists("shinySettings")) {
  writeLines("Using settings provided by user")
  shinyConfigPath <- getOption("CD-shiny-config", default = "config.yml")
  shinySettings <- loadShinySettings(shinyConfigPath)
}

normalizeColumnName <- function(data, possibleNames) {
  matches <- intersect(possibleNames, names(data))
  if (length(matches) == 0) {
    return(NULL)
  }
  matches[[1]]
}

queryTableSafe <- function(connection, schema, tableName, limit = 200L) {
  dbms <- tolower(connection@dbms)
  limit <- as.integer(max(1, limit))

  sql <- switch(dbms,
    "sql server" = "SELECT TOP @limit * FROM @schema.@tableName;",
    "oracle" = "SELECT * FROM @schema.@tableName WHERE ROWNUM <= @limit;",
    "sqlite" = "SELECT * FROM @schema.@tableName LIMIT @limit;",
    "postgresql" = "SELECT * FROM @schema.@tableName LIMIT @limit;",
    "redshift" = "SELECT * FROM @schema.@tableName LIMIT @limit;",
    "SELECT * FROM @schema.@tableName;"
  )

  tryCatch(
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = sql,
      schema = schema,
      tableName = tableName,
      limit = limit
    ),
    error = function(e) {
      warning(sprintf("Could not query table %s.%s: %s", schema, tableName, e$message), call. = FALSE)
      data.frame()
    }
  )
}

filterDiagnosticsData <- function(data, databaseIds, cohortIds) {
  if (nrow(data) == 0) {
    return(data)
  }

  databaseColumn <- normalizeColumnName(data, c("database_id", "databaseId"))
  cohortColumn <- normalizeColumnName(data, c("cohort_id", "cohortId"))

  if (!is.null(databaseColumn) && length(databaseIds) > 0) {
    data <- data[data[[databaseColumn]] %in% databaseIds, , drop = FALSE]
  }

  if (!is.null(cohortColumn) && length(cohortIds) > 0) {
    data <- data[data[[cohortColumn]] %in% cohortIds, , drop = FALSE]
  }

  data
}

emptyMessage <- function(data) {
  if (nrow(data) == 0) {
    return("No rows found for the current selection.")
  }
  ""
}

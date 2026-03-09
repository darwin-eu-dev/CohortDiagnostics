library(testthat)

test_that("DiagnosticsExplorer supports empty output states", {
  skip_if_not_installed("shinytest2")
  skip_if_not_installed("yaml")

  appDir <- tempfile("diagnostics-explorer-")
  dir.create(appDir)

  sourceAppDir <- system.file("shiny", "DiagnosticsExplorer", package = "CohortDiagnostics")
  file.copy(
    from = list.files(sourceAppDir, all.files = TRUE, full.names = TRUE, no.. = TRUE),
    to = appDir,
    recursive = TRUE,
    overwrite = TRUE
  )

  dataDir <- file.path(appDir, "data")
  dir.create(dataDir, showWarnings = FALSE, recursive = TRUE)
  sqliteDbPath <- file.path(dataDir, "MergedCohortDiagnosticsData.sqlite")

  connectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = "sqlite",
    server = sqliteDbPath
  )
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "
      CREATE TABLE IF NOT EXISTS main.database (
        database_id TEXT,
        database_name TEXT
      );

      CREATE TABLE IF NOT EXISTS main.cohort (
        cohort_id INTEGER,
        cohort_name TEXT
      );

      CREATE TABLE IF NOT EXISTS main.cohort_count (
        database_id TEXT,
        cohort_id INTEGER,
        cohort_entries INTEGER,
        cohort_subjects INTEGER
      );
    "
  )

  yaml::write_yaml(
    list(
      connectionDetails = list(
        dbms = "sqlite",
        server = "data/MergedCohortDiagnosticsData.sqlite"
      ),
      tablePrefix = "",
      resultsDatabaseSchema = "main",
      vocabularyDatabaseSchemas = "main",
      cohortTableName = "cohort",
      databaseTableName = "database"
    ),
    file = file.path(appDir, "config.yml")
  )

  app <- shinytest2::AppDriver$new(
    app_dir = appDir,
    name = "empty-output-smoke",
    height = 900,
    width = 1400
  )
  on.exit(app$stop(), add = TRUE)

  app$wait_for_value(output = "summary_empty")

  expect_match(app$get_value(output = "summary_empty"), "No rows found", fixed = TRUE)
  app$set_inputs(tabs = "Cohort Counts")
  app$wait_for_value(output = "cohort_counts_empty")
  expect_match(app$get_value(output = "cohort_counts_empty"), "No rows found", fixed = TRUE)

  app$set_inputs(tabs = "Table Browser")
  app$set_inputs(table_name = "cohort_count")
  app$wait_for_value(output = "table_preview_empty")
  expect_match(app$get_value(output = "table_preview_empty"), "No rows found", fixed = TRUE)
})

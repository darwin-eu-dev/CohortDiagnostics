library(testthat)

test_that("results data model helper outputs are well-formed", {
  specs <- getResultsDataModelSpecifications()
  expect_true(nrow(specs) > 0)
  expect_true(all(c("tableName", "columnName", "isRequired") %in% names(specs)))

  vocabTables <- getDefaultVocabularyTableNames()
  expect_true(length(vocabTables) > 0)
  expect_true(all(!grepl("_", vocabTables)))
})

test_that("getCdmDataSourceInformation validates required input", {
  expect_error(
    getCdmDataSourceInformation(connectionDetails = NULL, connection = NULL, cdmDatabaseSchema = "main"),
    "Please provide either connection or connectionDetails"
  )
})

test_that("getCdmDataSourceInformation handles missing and invalid cdm_source states", {
  dbFile <- tempfile(fileext = ".sqlite")
  on.exit(unlink(dbFile, force = TRUE), add = TRUE)

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = dbFile)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  expect_warning(
    outMissing <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "main"),
    "CDM Source table not found"
  )
  expect_null(outMissing)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "CREATE TABLE main.cdm_source (
      cdm_source_name TEXT,
      source_description TEXT,
      source_release_date TEXT,
      cdm_release_date TEXT,
      cdm_version TEXT,
      vocabulary_version TEXT
    );"
  )

  expect_warning(
    outEmpty <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "main"),
    "does not have any records"
  )
  expect_null(outEmpty)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "INSERT INTO main.cdm_source VALUES
      ('A', 'descA', '2020-01-01', '2020-01-01', '5.4', 'v1'),
      ('B', 'descB', '2021-01-01', '2021-01-01', '5.4', 'v2');"
  )

  expect_warning(
    outMulti <- getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "main"),
    "has more than one record"
  )
  expect_null(outMulti)
})

test_that("getCdmDataSourceInformation returns expected metadata for one source row", {
  dbFile <- tempfile(fileext = ".sqlite")
  on.exit(unlink(dbFile, force = TRUE), add = TRUE)

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = dbFile)
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "CREATE TABLE main.cdm_source (
      cdm_source_name TEXT,
      source_description TEXT,
      source_release_date TEXT,
      cdm_release_date TEXT,
      cdm_version TEXT,
      vocabulary_version TEXT
    );
    INSERT INTO main.cdm_source VALUES
      ('Example Source', 'Example Description', '2022-05-10', '2022-05-11', '5.4', 'v5.0');"
  )

  out <- suppressWarnings(getCdmDataSourceInformation(connection = connection, cdmDatabaseSchema = "main"))

  expect_s3_class(out, "tbl_df")
  expect_equal(nrow(out), 1)
  expect_equal(out$cdmSourceName[[1]], "Example Source")
  expect_equal(out$sourceDescription[[1]], "Example Description")
  expect_equal(out$cdmVersion[[1]], "5.4")
  expect_equal(out$vocabularyVersion[[1]], "v5.0")
})

test_that("executeDiagnostics validates required cohort fields", {
  exportFolder <- tempfile("cd-export-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  cohortDefinitionSet <- data.frame(
    cohortId = 1,
    cohortName = "Test cohort",
    sql = "SELECT 1;"
  )

  expect_error(
    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      exportFolder = exportFolder,
      databaseId = "test_db",
      cohortDatabaseSchema = "main",
      cdmDatabaseSchema = "main",
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runTimeSeries = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortRelationship = FALSE,
      runTemporalCohortCharacterization = FALSE,
      temporalCovariateSettings = list(getDefaultCovariateSettings())
    ),
    "json"
  )
})

test_that("executeDiagnostics stops early when no cohorts remain after filtering", {
  exportFolder <- tempfile("cd-export-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  cohortDefinitionSet <- data.frame(
    cohortId = integer(),
    cohortName = character(),
    json = character(),
    sql = character(),
    stringsAsFactors = FALSE
  )

  expect_error(
    executeDiagnostics(
      cohortDefinitionSet = cohortDefinitionSet,
      exportFolder = exportFolder,
      databaseId = "test_db",
      cohortDatabaseSchema = "main",
      cdmDatabaseSchema = "main",
      runInclusionStatistics = FALSE,
      runIncludedSourceConcepts = FALSE,
      runOrphanConcepts = FALSE,
      runTimeSeries = FALSE,
      runVisitContext = FALSE,
      runBreakdownIndexEvents = FALSE,
      runIncidenceRate = FALSE,
      runCohortRelationship = FALSE,
      runTemporalCohortCharacterization = FALSE,
      temporalCovariateSettings = list(getDefaultCovariateSettings())
    ),
    "No cohorts specified"
  )
})

library(testthat)

createSqliteTestConnection <- function() {
  sqliteFile <- tempfile(fileext = ".sqlite")
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "sqlite", server = sqliteFile)
  connection <- DatabaseConnector::connect(connectionDetails)
  list(connection = connection, sqliteFile = sqliteFile)
}

test_that("cohort-level counting functions work against sqlite", {
  setup <- createSqliteTestConnection()
  connection <- setup$connection
  sqliteFile <- setup$sqliteFile
  on.exit({
    DatabaseConnector::disconnect(connection)
    unlink(sqliteFile, force = TRUE)
  }, add = TRUE)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "
      CREATE TABLE main.cohort (
        cohort_definition_id INTEGER,
        subject_id INTEGER,
        cohort_start_date TEXT,
        cohort_end_date TEXT
      );
      INSERT INTO main.cohort VALUES
        (1, 10, '2020-01-01', '2020-01-10'),
        (1, 11, '2020-01-02', '2020-01-11'),
        (1, 10, '2020-02-01', '2020-02-10'),
        (2, 12, '2020-03-01', '2020-03-10');
    "
  )

  counts <- CohortDiagnostics:::getCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohortIds = c(1, 2, 3)
  )

  expect_equal(nrow(counts), 3)
  expect_equal(counts$cohortEntries[counts$cohortId == 1], 3)
  expect_equal(counts$cohortSubjects[counts$cohortId == 1], 2)
  expect_equal(counts$cohortEntries[counts$cohortId == 3], 0)

  expect_true(CohortDiagnostics:::checkIfCohortInstantiated(connection, "main", "cohort", 1))
  expect_false(CohortDiagnostics:::checkIfCohortInstantiated(connection, "main", "cohort", 999))

  exportFolder <- tempfile("cd-counts-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  cohortCounts <- CohortDiagnostics:::computeCohortCounts(
    connection = connection,
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cohorts = dplyr::tibble(cohortId = c(1, 2, 3)),
    exportFolder = exportFolder,
    minCellCount = 5,
    databaseId = "db_sqlite",
    writeResult = TRUE
  )

  expect_true(file.exists(file.path(exportFolder, "cohort_count.csv")))
  expect_true(all(c("cohortId", "cohortEntries", "cohortSubjects", "databaseId") %in% names(cohortCounts)))
})

test_that("metadata and vocabulary utilities write expected outputs", {
  setup <- createSqliteTestConnection()
  connection <- setup$connection
  sqliteFile <- setup$sqliteFile
  on.exit({
    DatabaseConnector::disconnect(connection)
    unlink(sqliteFile, force = TRUE)
  }, add = TRUE)

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = "
      CREATE TABLE main.vocabulary (
        vocabulary_id TEXT,
        vocabulary_version TEXT
      );
      INSERT INTO main.vocabulary VALUES
        ('None', 'v5.0'),
        ('None', 'v5.1'),
        ('SNOMED', 'ignored');
    "
  )

  vocabularyVersion <- CohortDiagnostics:::getVocabularyVersion(connection, "main")
  expect_match(vocabularyVersion, "v5\\.0")
  expect_match(vocabularyVersion, "v5\\.1")

  exportFolder <- tempfile("cd-metadata-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  CohortDiagnostics:::saveDatabaseMetaData(
    databaseId = "db_x",
    databaseName = NA_character_,
    databaseDescription = NA_character_,
    exportFolder = exportFolder,
    minCellCount = 5,
    vocabularyVersionCdm = c("cdm_a", "cdm_b"),
    vocabularyVersion = c("vocab_a", "vocab_b")
  )

  metadata <- readr::read_csv(file.path(exportFolder, "database.csv"), col_types = readr::cols())
  expect_equal(metadata$database_name[[1]], "db_x")
  expect_equal(metadata$description[[1]], "db_x")
  expect_equal(metadata$vocabulary_version_cdm[[1]], "cdm_a;cdm_b")
  expect_equal(metadata$vocabulary_version[[1]], "vocab_a;vocab_b")
})

test_that("exportCharacterization handles missing and populated outputs", {
  emptyCharacteristics <- list()
  expect_warning(
    CohortDiagnostics:::exportCharacterization(
      characteristics = emptyCharacteristics,
      databaseId = "db_1",
      incremental = TRUE,
      covariateValueFileName = tempfile(fileext = ".csv"),
      covariateValueContFileName = tempfile(fileext = ".csv"),
      covariateRefFileName = tempfile(fileext = ".csv"),
      analysisRefFileName = tempfile(fileext = ".csv"),
      timeRefFileName = tempfile(fileext = ".csv"),
      counts = dplyr::tibble(),
      minCellCount = 5
    ),
    "No characterization output"
  )

  outputFolder <- tempfile("cd-char-")
  dir.create(outputFolder)
  on.exit(unlink(outputFolder, recursive = TRUE, force = TRUE), add = TRUE)

  characteristics <- list(
    covariates = dplyr::tibble(
      cohortId = 1,
      timeId = 0,
      covariateId = 100,
      sumValue = 3,
      mean = 0.02,
      sd = 0.4
    ),
    covariateRef = dplyr::tibble(
      covariateId = 100,
      covariateName = "x",
      analysisId = 1,
      conceptId = 200
    )
    ,
    analysisRef = dplyr::tibble(
      analysisId = 1,
      analysisName = "a",
      domainId = "Condition",
      isBinary = "Y"
    ),
    timeRef = dplyr::tibble(timeId = 0, startDay = 0, endDay = 30)
  )
  counts <- dplyr::tibble(
    cohortId = 1,
    databaseId = "db_1",
    cohortEntries = 100,
    cohortSubjects = 80
  )

  filePaths <- list(
    covariateValue = file.path(outputFolder, "temporal_covariate_value.csv"),
    covariateValueDist = file.path(outputFolder, "temporal_covariate_value_dist.csv"),
    covariateRef = file.path(outputFolder, "temporal_covariate_ref.csv"),
    analysisRef = file.path(outputFolder, "temporal_analysis_ref.csv"),
    timeRef = file.path(outputFolder, "temporal_time_ref.csv")
  )

  CohortDiagnostics:::exportCharacterization(
    characteristics = characteristics,
    databaseId = "db_1",
    incremental = TRUE,
    covariateValueFileName = filePaths$covariateValue,
    covariateValueContFileName = filePaths$covariateValueDist,
    covariateRefFileName = filePaths$covariateRef,
    analysisRefFileName = filePaths$analysisRef,
    timeRefFileName = filePaths$timeRef,
    counts = counts,
    minCellCount = 5
  )

  filtered <- readr::read_csv(filePaths$covariateValue, col_types = readr::cols())
  expect_equal(filtered$sum_value[[1]], -5)
  expect_equal(filtered$mean[[1]], -0.05)
  expect_equal(filtered$sd[[1]], 0)
  expect_true(file.exists(filePaths$covariateValue))
  expect_true(file.exists(filePaths$covariateRef))
  expect_true(file.exists(filePaths$analysisRef))
  expect_true(file.exists(filePaths$timeRef))
  expect_false(file.exists(filePaths$covariateValueDist))
})

test_that("visit context functions execute branch logic with mocked DB calls", {
  mockConnection <- getMockConnection()
  executeCount <- 0
  cleanupCount <- 0

  local_mocked_bindings(
    loadRenderTranslateSql = function(...) {
      "sql"
    },
    .package = "SqlRender"
  )
  local_mocked_bindings(
    executeSql = function(...) {
      executeCount <<- executeCount + 1
      invisible(NULL)
    },
    renderTranslateQuerySql = function(...) {
      dplyr::tibble(cohortId = 1, visitConceptId = 9201, visitContext = "Before", subjects = 4)
    },
    renderTranslateExecuteSql = function(...) {
      cleanupCount <<- cleanupCount + 1
      invisible(NULL)
    },
    .package = "DatabaseConnector"
  )

  expect_warning(
    visitContext <- CohortDiagnostics:::getVisitContext(
      connection = mockConnection,
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cohortIds = c(1, 2),
      conceptIdTable = "#concept_ids",
      cdmVersion = 4
    ),
    "Only OMOP CDM v5.x.x is supported"
  )
  expect_equal(nrow(visitContext), 1)
  expect_equal(executeCount, 1)
  expect_equal(cleanupCount, 2)
})

test_that("executeVisitContextDiagnostics runs only for required subset", {
  writeCalled <- FALSE
  recordCalled <- FALSE

  local_mocked_bindings(
    subsetToRequiredCohorts = function(...) {
      dplyr::tibble(cohortId = 1, checksum = "abc")
    },
    getVisitContext = function(...) {
      dplyr::tibble(cohortId = 1, visitConceptId = 9201, visitContext = "After", subjects = 1)
    },
    makeDataExportable = function(x, ...) {
      x
    },
    writeToCsv = function(...) {
      writeCalled <<- TRUE
      invisible(NULL)
    },
    recordTasksDone = function(...) {
      recordCalled <<- TRUE
      invisible(NULL)
    },
    .package = "CohortDiagnostics"
  )

  CohortDiagnostics:::executeVisitContextDiagnostics(
    connection = getMockConnection(),
    tempEmulationSchema = NULL,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    cdmVersion = 5,
    databaseId = "db_1",
    exportFolder = tempdir(),
    minCellCount = 5,
    cohorts = dplyr::tibble(cohortId = 1, checksum = "abc"),
    instantiatedCohorts = 1,
    recordKeepingFile = tempfile(fileext = ".csv"),
    incremental = TRUE
  )

  expect_true(writeCalled)
  expect_true(recordCalled)
})

test_that("incidence helper logic covers error, empty and aggregation branches", {
  expect_error(
    CohortDiagnostics:::getIncidenceRate(
      connection = getMockConnection(),
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cdmDatabaseSchema = "main",
      cohortId = 1,
      cdmVersion = 4
    ),
    "Only CDM version 5 is supported"
  )

  local_mocked_bindings(
    checkIfCohortInstantiated = function(...) {
      FALSE
    },
    .package = "CohortDiagnostics"
  )
  expect_warning(
    noData <- CohortDiagnostics:::getIncidenceRate(
      connection = getMockConnection(),
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      cdmDatabaseSchema = "main",
      cohortId = 100,
      cdmVersion = 5
    ),
    "appears to be empty"
  )
  expect_equal(nrow(noData), 0)

  recoded <- CohortDiagnostics:::recode(dplyr::tibble(
    calendarYear = c(2019, 2020),
    ageGroup = c(3, 4),
    gender = c("male", "female"),
    cohortCount = c(1, 2),
    personYears = c(10, 20)
  ))
  expect_equal(recoded$ageGroup, c("30-39", "40-49"))
  expect_equal(recoded$gender, c("Male", "Female"))

  aggregated <- CohortDiagnostics:::aggregateIr(
    ratesSummary = recoded,
    aggregateList = list(gender = recoded$gender)
  )
  expect_equal(sum(aggregated$cohortCount), 3)
  expect_equal(sum(aggregated$personYears), 30)

  emptyAggregated <- CohortDiagnostics:::aggregateIr(
    ratesSummary = dplyr::tibble(),
    aggregateList = list()
  )
  expect_equal(nrow(emptyAggregated), 0)
})

test_that("computeIncidenceRates executes required branch and records completion", {
  writeCalled <- FALSE
  recordCalled <- FALSE

  local_mocked_bindings(
    subsetToRequiredCohorts = function(...) {
      dplyr::tibble(cohortId = 1, cohortName = "c1", checksum = "abc")
    },
    getIncidenceRate = function(...) {
      dplyr::tibble(
        cohortCount = 2,
        personYears = 10,
        incidenceRate = 200
      )
    },
    makeDataExportable = function(x, ...) {
      x
    },
    enforceMinCellValue = function(data, ...) {
      data
    },
    writeToCsv = function(...) {
      writeCalled <<- TRUE
      invisible(NULL)
    },
    recordTasksDone = function(...) {
      recordCalled <<- TRUE
      invisible(NULL)
    },
    .package = "CohortDiagnostics"
  )

  CohortDiagnostics:::computeIncidenceRates(
    connection = getMockConnection(),
    tempEmulationSchema = NULL,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    databaseId = "db_1",
    exportFolder = tempdir(),
    minCellCount = 5,
    cohorts = dplyr::tibble(cohortId = 1, cohortName = "c1", checksum = "abc"),
    instantiatedCohorts = c(1, 2),
    recordKeepingFile = tempfile(fileext = ".csv"),
    washoutPeriod = 365,
    incremental = TRUE
  )

  expect_true(writeCalled)
  expect_true(recordCalled)
})

test_that("time series and concept set diagnostics early guards are covered", {
  expect_warning(
    out <- CohortDiagnostics:::runCohortTimeSeriesDiagnostics(
      connection = getMockConnection(),
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      runCohortTimeSeries = FALSE,
      runDataSourceTimeSeries = FALSE
    ),
    "Both Cohort Time Series and Data Source Time Series are set to FALSE"
  )
  expect_null(out)

  local_mocked_bindings(
    andromeda = function(...) {
      list()
    },
    .package = "Andromeda"
  )
  local_mocked_bindings(
    renderTranslateQuerySql = function(...) {
      dplyr::tibble()
    },
    .package = "CohortDiagnostics"
  )
  expect_warning(
    noCohorts <- CohortDiagnostics:::runCohortTimeSeriesDiagnostics(
      connection = getMockConnection(),
      cdmDatabaseSchema = "main",
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      runCohortTimeSeries = TRUE,
      runDataSourceTimeSeries = FALSE,
      cohortIds = c(1)
    ),
    "Please check if cohorts are instantiated"
  )
  expect_null(noCohorts)

  cohorts <- dplyr::tibble(
    cohortId = 1,
    cohortName = "c1",
    cohortFullName = "c1",
    sql = "SELECT 1 as codeset_id, c.concept_id FROM (SELECT 1 AS concept_id) C",
    json = makeConceptSetJson(c(1), TRUE),
    checksum = "abc",
    isSubset = FALSE,
    subsetParent = NA_integer_
  )

  local_mocked_bindings(
    combineConceptSetsFromCohorts = function(...) {
      data.frame()
    },
    .package = "CohortDiagnostics"
  )
  expect_null(CohortDiagnostics:::runConceptSetDiagnostics(
    connection = getMockConnection(),
    tempEmulationSchema = NULL,
    cdmDatabaseSchema = "main",
    databaseId = "db_1",
    cohorts = cohorts,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = FALSE,
    runBreakdownIndexEvents = FALSE,
    exportFolder = tempdir(),
    minCellCount = 5,
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    recordKeepingFile = tempfile(fileext = ".csv")
  ))
})

test_that("runConceptSetDiagnostics rejects unsupported external concept counts path", {
  local_mocked_bindings(
    combineConceptSetsFromCohorts = function(...) {
      dplyr::tibble(
        uniqueConceptSetId = 1,
        cohortId = 1,
        conceptSetId = 1,
        conceptSetName = "set_1",
        conceptSetExpression = "{}",
        conceptSetSql = "SELECT 1 as codeset_id, c.concept_id FROM (SELECT 1 AS concept_id) C"
      )
    },
    subsetToRequiredCohorts = function(...) {
      dplyr::tibble(cohortId = 1, checksum = "abc")
    },
    instantiateUniqueConceptSets = function(...) {
      invisible(NULL)
    },
    createConceptCountsTable = function(...) {
      invisible(NULL)
    },
    timeExecution = function(exportFolder, taskName, cohortIds = NULL, parent = NULL, expr = NULL, start = NULL, execTime = NULL) {
      if (!is.null(expr)) {
        force(expr)
      }
      dplyr::tibble()
    },
    .package = "CohortDiagnostics"
  )

  expect_error(
    CohortDiagnostics:::runConceptSetDiagnostics(
      connection = getMockConnection(),
      tempEmulationSchema = NULL,
      cdmDatabaseSchema = "main",
      databaseId = "db_1",
      cohorts = dplyr::tibble(
        cohortId = 1,
        cohortName = "c1",
        cohortFullName = "c1",
        sql = "SELECT 1 as codeset_id, c.concept_id FROM (SELECT 1 AS concept_id) C",
        json = makeConceptSetJson(c(1), TRUE),
        checksum = "abc",
        isSubset = FALSE,
        subsetParent = NA_integer_
      ),
      runIncludedSourceConcepts = TRUE,
      runOrphanConcepts = FALSE,
      runBreakdownIndexEvents = FALSE,
      exportFolder = tempdir(),
      minCellCount = 5,
      cohortDatabaseSchema = "main",
      cohortTable = "cohort",
      useExternalConceptCountsTable = TRUE,
      incremental = TRUE,
      conceptIdTable = NULL,
      recordKeepingFile = tempfile(fileext = ".csv")
    ),
    "external concept count table is not supported"
  )
})

test_that("concept id diagnostics helpers run with mocked SQL plumbing", {
  local_mocked_bindings(
    loadRenderTranslateSql = function(...) {
      "sql"
    },
    .package = "SqlRender"
  )
  executeCount <- 0
  local_mocked_bindings(
    executeSql = function(...) {
      executeCount <<- executeCount + 1
      invisible(NULL)
    },
    renderTranslateQuerySql = function(...) {
      dplyr::tibble(conceptId = c(1, 2), codesetId = c(10, 10), conceptCount = c(3, 4), conceptSubjects = c(1, 2))
    },
    .package = "DatabaseConnector"
  )

  CohortDiagnostics:::createConceptTable(getMockConnection(), tempEmulationSchema = NULL)
  CohortDiagnostics:::createConceptCountsTable(
    connection = getMockConnection(),
    cdmDatabaseSchema = "main",
    tempEmulationSchema = NULL,
    conceptCountsDatabaseSchema = "main",
    conceptCountsTable = "concept_counts",
    conceptCountsTableIsTemp = FALSE
  )
  orphans <- CohortDiagnostics:::.findOrphanConcepts(
    connection = getMockConnection(),
    cdmDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    tempEmulationSchema = NULL,
    conceptIds = c(1, 2),
    useCodesetTable = FALSE,
    conceptCountsDatabaseSchema = "main",
    conceptCountsTable = "concept_counts",
    conceptCountsTableIsTemp = FALSE
  )

  expect_true(executeCount >= 3)
  expect_equal(nrow(orphans), 2)
})

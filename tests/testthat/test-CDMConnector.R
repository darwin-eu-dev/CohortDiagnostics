test_that("CDMConnector interface works", {
    
  # Run CohortDiagnostics using CDMConnector
  
  library(Eunomia)
  library(CohortDiagnostics)
  library(CohortGenerator)
  library(CDMConnector)

  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  cohortTable <- "mycohort"
  conceptCountsTable <- "concept_counts"
  outputFolder <- tempfile()
  fs::dir_create(outputFolder)

  databaseId <- "Eunomia"
  minCellCount <- 5

  # cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  #   settingsFileName = "Cohorts.csv",
  #   jsonFolder = "cohorts",
  #   sqlFolder = "sql/sql_server",
  #   packageName = "CohortDiagnostics"
  # )
  
  # expect_true(T)
  
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = cdmDatabaseSchema, writeSchema = cohortDatabaseSchema, cdmName = databaseId)
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  CohortDiagnostics::createConceptCountsTable(connection = attr(cdm, "dbcon"),
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsTable = conceptCountsTable)

  invisible(capture.output(
    CohortDiagnostics::executeDiagnosticsCdm(cdm = cdm,
                                             cohortDefinitionSet = cohortDefinitionSet,
                                             cohortTable = cohortTable,
                                             conceptCountsTable = conceptCountsTable,
                                             exportFolder = outputFolder,
                                             minCellCount = minCellCount,
                                             runInclusionStatistics = T,
                                             runIncludedSourceConcepts = T,
                                             runOrphanConcepts = T,
                                             runTimeSeries = T,
                                             runVisitContext = T,
                                             runBreakdownIndexEvents = T,
                                             runIncidenceRate = T,
                                             runCohortRelationship = T,
                                             runTemporalCohortCharacterization = T,
                                             useExternalConceptCountsTable = T)
  ))


  expect_true(length(list.files(outputFolder)) > 10)

  # package results ----

  sqliteFile <- tempfile(fileext = ".sqlite")

  expect_false(file.exists(sqliteFile))

  invisible(capture.output(
    CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder,
                                               sqliteDbPath = sqliteFile,
                                               overwrite = TRUE)
  ))

  expect_true(file.exists(sqliteFile))
  
  # Launch diagnostics explorer shiny app ----
  # CohortDiagnostics::launchDiagnosticsExplorer()

})




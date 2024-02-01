
test_that("cdm interface works", {
  
  exportFolder <- tempfile()
  fs::dir_create(exportFolder)
  
  cohortTable <- "mycohort"
  con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir()))
  
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  
  cohortSet <- CDMConnector::readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  
  # debugonce(executeDiagnosticsCdm)
  invisible(capture.output(
    executeDiagnosticsCdm(cdm = cdm,
                          cohortSet = cohortSet,
                          exportFolder = exportFolder)
  ))
  
  expected_files <- c("cohort_count.csv", "cohort.csv", "database.csv", "executionTimes.csv", 
                      "log.txt", "metadata.csv", "Results_eunomia.zip")
  
  expect_true(all(expected_files %in% list.files(exportFolder)))
  
  DBI::dbDisconnect(con, shutdown = TRUE)
})


test_that("cdm interface works with ohdsi cohort set", {
  # Run CohortDiagnostics using CDMConnector
  
  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  cohortTable <- "mycohort"
  conceptCountsTable <- "concept_counts"
  outputFolder <- "export"
  databaseId <- "Eunomia"
  minCellCount <- 5
  
  cohortDefinitionSet <-
    CohortGenerator::getCohortDefinitionSet(
      settingsFileName = "settings/CohortsToCreate.csv",
      jsonFolder = "cohorts",
      sqlFolder = "sql/sql_server",
      packageName = "SkeletonCohortDiagnosticsStudy",
      cohortFileNameValue = "cohortId"
    ) %>% dplyr::tibble()
  
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = cdmDatabaseSchema, writeSchema = cohortDatabaseSchema, cdmName = databaseId)
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  
  CohortDiagnostics::createConceptCountsTable(connection = attr(cdm, "dbcon"),
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsTable = conceptCountsTable)
  
  CohortDiagnostics::executeDiagnosticsCdm(cdm = cdm,
                                           cohortSet = cohortDefinitionSet,
                                           # conceptCountsTable = conceptCountsTable,
                                           exportFolder = outputFolder,
                                           minCellCount = minCellCount)
  
  # package results ----
  CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
  # Launch diagnostics explorer shiny app ----
  CohortDiagnostics::launchDiagnosticsExplorer()
  
})

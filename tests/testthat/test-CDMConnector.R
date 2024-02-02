test_that("cdm interface works", {
  exportFolder <- tempfile()
  cohortTable  <- "mycohort"
  conceptCountsTable <- "conceptCounts"
  cohortDefSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% c(14906))
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefSet, name = cohortTable)
  
  CohortDiagnostics::createConceptCountsTable(connection = attr(cdm, "dbcon"),
                                              cdmDatabaseSchema = "main",
                                              conceptCountsDatabaseSchema = "main",
                                              conceptCountsTable = conceptCountsTable)
  
  invisible(capture.output(
    executeDiagnosticsCdm(cdm = cdm,
                          cohortDefinitionSet = cohortDefSet,
                          cohortTable = cohortTable,
                          conceptCountsTable = conceptCountsTable,
                          exportFolder = exportFolder,
                          useExternalConceptCountsTable = TRUE)
  ))
  
  expected_files <- c("cohort_count.csv", "cohort.csv", "database.csv", "executionTimes.csv", 
                      "log.txt", "metadata.csv", "Results_eunomia.zip")
  
  expect_true(all(expected_files %in% list.files(exportFolder)))
  
  DBI::dbDisconnect(con, shutdown = TRUE)
})
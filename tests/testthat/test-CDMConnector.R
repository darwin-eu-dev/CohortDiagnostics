test_that("cdm interface works", {
  exportFolder <- tempfile()
  cohortTable  <- "mycohort"
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  
  invisible(capture.output(
    executeDiagnosticsCdm(cdm = cdm,
                          cohortDefinitionSet = cohortDefinitionSet,
                          cohortTable = cohortTable,
                          exportFolder = exportFolder,
                          useExternalConceptCountsTable = FALSE)
  ))
  
  expected_files <- c("cohort_count.csv", "cohort.csv", "database.csv", "executionTimes.csv", 
                      "log.txt", "metadata.csv", "Results_eunomia.zip")
  
  expect_true(all(expected_files %in% list.files(exportFolder)))
  
  DBI::dbDisconnect(con, shutdown = TRUE)
})
test_that("CDMConnector interface works", {
    
  cdmDatabaseSchema <- "main"
  cohortDatabaseSchema <- "main"
  cohortTable <- "mycohort"
  conceptCountsTable <- "concept_counts"
  outputFolder <- tempfile()
  fs::dir_create(outputFolder)

  cohortDefinitionSet <- CDMConnector::readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  
  con <- DBI::dbConnect(duckdb::duckdb(dbdir = CDMConnector::eunomia_dir()))
  cdm <- CDMConnector::cdmFromCon(con, 
                                  cdmSchema = cdmDatabaseSchema, 
                                  writeSchema = cohortDatabaseSchema, 
                                  cdmName = "Eunomia")
  
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  
  CohortDiagnostics::createConceptCountsTable(connection = con,
                                              cdmDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsDatabaseSchema = cdmDatabaseSchema,
                                              conceptCountsTable = conceptCountsTable)

  # debugonce(executeDiagnosticsCdm)
  invisible(capture.output(
    executeDiagnosticsCdm(cdm = cdm,
                          cohortDefinitionSet = cohortDefinitionSet,
                          cohortTable = cohortTable,
                          conceptCountsTable = conceptCountsTable,
                          exportFolder = outputFolder,
                          minCellCount = 5)
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
 



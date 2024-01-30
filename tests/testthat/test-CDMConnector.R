
test_that("cdm interface works", {
  
  exportFolder <- tempfile()
  if(!dir.exists(exportFolder)) dir.create(exportFolder)
  
  cohortTable <- "mycohort"
  con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir()))
  
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  
  cohortSet <- CDMConnector::readCohortSet(system.file("cohorts1", package = "CDMConnector"))
  
  # debugonce(executeDiagnosticsCdm)
  executeDiagnosticsCdm(cdm = cdm,
                        cohortSet = cohortSet,
                        exportFolder = exportFolder)
  
  DBI::dbDisconnect(con, shutdown = TRUE)
})


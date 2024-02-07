
test_that("cdm interface works", {
  
  exportFolder <- tempfile()
  if(!dir.exists(exportFolder)) dir.create(exportFolder)
  
  cohortTable <- "mycohort"
  CDMConnector::example_datasets()
  con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir("synthea-snf-10k")))
  
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  
  cohortSet <- CDMConnector::readCohortSet(system.file("cohorts2", package = "CDMConnector"))
  
  # debugonce(executeDiagnosticsCdm)
  executeDiagnosticsCdm(cdm = cdm,
                        cohortSet = cohortSet,
                        runAnalysis = 1,
                        exportFolder = exportFolder)
  
  
  expect_true("log.txt" %in% list.files(exportFolder))
  expect_true("cohort_count.csv" %in% list.files(exportFolder))
  expect_true("cohort.csv" %in% list.files(exportFolder))
  expect_true("database.csv" %in% list.files(exportFolder))
  expect_true("metadata.csv" %in% list.files(exportFolder))
  
  DBI::dbDisconnect(con, shutdown = TRUE)
})



# test_that("exportConceptInformation", {
#   
#   exportConceptInformation(
#     connection = connection,
#     cdmDatabaseSchema = cdmDatabaseSchema,
#     tempEmulationSchema = tempEmulationSchema,
#     conceptIdTable = "#concept_ids",
#     incremental = incremental,
#     exportFolder = exportFolder
#   )
#   
# })



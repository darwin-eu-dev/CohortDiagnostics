
test_that("cdm interface works", {
  
  outputFolder <- tempfile()
  if(!dir.exists(outputFolder)) dir.create(outputFolder)
  
  cohortTable <- "mycohort"
  con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir()))
  
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
  
  cohortDefinitionSet <- CDMConnector::readCohortSet(system.file("cohorts", package = "CohortDiagnostics")) %>% 
    dplyr::mutate(cohortName = cohort_name, 
                  sql = "adsf", 
                  json = as.character(json),
                  cohortId = as.numeric(cohort_definition_id))
  
  
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  
  # debugonce(executeDiagnosticsCdm)
  executeDiagnosticsCdm(cdm = cdm,
                        cohortDefinitionSet = cohortDefinitionSet,
                        cohortTable = cohortTable,
                        exportFolder = outputFolder)
})


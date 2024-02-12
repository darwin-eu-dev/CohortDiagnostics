# Calculate Charlson Index score - example script

library(Eunomia)
library(CohortDiagnostics)
library(CohortGenerator)
library(CDMConnector)

cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "mycohort"
conceptCountsTable <- "concept_counts"
outputFolder <- "export"
databaseId <- "Eunomia"
minCellCount <- 5

if (!dir.exists(outputFolder)) {
  dir.create(outputFolder)
}

cohortDefinitionSet <- CohortGenerator::getCohortDefinitionSet(
  settingsFileName = "Cohorts.csv",
  jsonFolder = "cohorts",
  sqlFolder = "sql/sql_server",
  packageName = "CohortDiagnostics"
)

connectAndExecuteSql <- function(aggregated = TRUE) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
  cdm <- CDMConnector::cdmFromCon(con, cdmSchema = cdmDatabaseSchema, writeSchema = cohortDatabaseSchema, cdmName = databaseId)
  cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
  
  # aggregated
  CohortDiagnostics:::renderTranslateExecuteSql(con,
                                                SqlRender::readSql(system.file("sql", "sql_server", "CharlsonIndex.sql", package = "CohortDiagnostics")),
                                                aggregated = aggregated,
                                                temporal = TRUE,
                                                cdm_database_schema = "main",
                                                cohort_table = cohortTable,
                                                covariate_table = "covariate",
                                                analysis_id = 901,
                                                included_cov_table = "",
                                                cohort_definition_id = 17492,
                                                analysis_name = "CharlsonIndex",
                                                domain_id = "Condition",
                                                row_id_field = "subject_id")
  result <- DBI::dbGetQuery(con, "select * from covariate")
  print(head(result))
  DBI::dbDisconnect(con)
}

connectAndExecuteSql(aggregated = TRUE)
connectAndExecuteSql(aggregated = FALSE)

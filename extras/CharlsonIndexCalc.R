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

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
cdm <- CDMConnector::cdmFromCon(con, cdmSchema = cdmDatabaseSchema, writeSchema = cohortDatabaseSchema, cdmName = databaseId)
cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)

CohortDiagnostics:::renderTranslateExecuteSql(con,
                                              SqlRender::readSql(system.file("sql", "sql_server", "CharlsonIndex.sql", package = "CohortDiagnostics")),
                                              aggregated = TRUE,
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

con <- attr(cdm, "dbcon")
DBI::dbGetQuery(con, "select * from covariate")
# cohort_definition_id covariate_id time_id count_value min_value max_value average_value standard_deviation median_value p10_value p25_value p75_value p90_value
# 1                17492         1901      NA         316         0         2          0.82               0.43            1         0         0         1         2

DBI::dbDisconnect(con)
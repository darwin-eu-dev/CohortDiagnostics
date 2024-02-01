# Run CohortDiagnostics using CDMConnector

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

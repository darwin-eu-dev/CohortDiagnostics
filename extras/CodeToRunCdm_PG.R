# Run CohortDiagnostics using CDMConnector on a PG db

library(Eunomia)
library(CohortDiagnostics)
library(CohortGenerator)
library(CDMConnector)

cdmDatabaseSchema <- Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA")
cohortDatabaseSchema <- Sys.getenv("LOCAL_POSTGRESQL_OHDSI_SCHEMA")
tablePrefix <- "cdd_"
cohortTable <- "mycohort"
conceptCountsTable <- "concept_counts"
outputFolder <- "export"
databaseId <- "Local_PG"
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

con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname = "synthea10",
                      host = "localhost",
                      user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
                      password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD"),
                      bigint = "integer")

cdm <- CDMConnector::cdm_from_con(con, 
                                  cdm_schema = cdmDatabaseSchema, 
                                  write_schema = c(schema = cohortDatabaseSchema, prefix = tablePrefix),
                                  cdm_name = databaseId)

cdm <- CDMConnector::generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)

# only CDMConnector functions use prefix directly, for other functions, we need to add it
cohortTable <- paste0(tablePrefix, "mycohort")
conceptCountsTable <- paste0(tablePrefix, "concept_counts")

CohortDiagnostics::createConceptCountsTable(connection = attr(cdm, "dbcon"),
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            conceptCountsDatabaseSchema = cohortDatabaseSchema,
                                            conceptCountsTable = conceptCountsTable)

CohortDiagnostics::executeDiagnosticsCdm(cdm = cdm,
                                         cohortDefinitionSet = cohortDefinitionSet,
                                         cohortTable = cohortTable,
                                         conceptCountsTable = conceptCountsTable,
                                         exportFolder = outputFolder,
                                         minCellCount = minCellCount,
                                         runInclusionStatistics = T,
                                         runIncludedSourceConcepts = T,
                                         runOrphanConcepts = T,
                                         runTimeSeries = T,
                                         runVisitContext = T,
                                         runBreakdownIndexEvents = T,
                                         runIncidenceRate = T,
                                         runCohortRelationship = T,
                                         runTemporalCohortCharacterization = T,
                                         useExternalConceptCountsTable = T)

# package results ----
CohortDiagnostics::createMergedResultsFile(dataFolder = outputFolder, overwrite = TRUE)
# Launch diagnostics explorer shiny app ----
CohortDiagnostics::launchDiagnosticsExplorer()

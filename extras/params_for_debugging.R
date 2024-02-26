
getDefaultCovariateSettings <- function() {
  FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsIndexYearMonth = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useConditionOccurrence = TRUE,
    useProcedureOccurrence = TRUE,
    useDrugEraStart = TRUE,
    useMeasurement = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = FALSE,
    useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = TRUE,
    useDcsi = TRUE,
    useChads2 = TRUE,
    useChads2Vasc = TRUE,
    useHfrs = FALSE,
    temporalStartDays = c(
      # components displayed in cohort characterization
      -9999, # anytime prior
      -365, # long term prior
      -180, # medium term prior
      -30, # short term prior
      
      # components displayed in temporal characterization
      -365, # one year prior to -31
      -30, # 30 day prior not including day 0
      0, # index date only
      1, # 1 day after to day 30
      31,
      -9999 # Any time prior to any time future
    ),
    temporalEndDays = c(
      0, # anytime prior
      0, # long term prior
      0, # medium term prior
      0, # short term prior
      
      # components displayed in temporal characterization
      -31, # one year prior to -31
      -1, # 30 day prior not including day 0
      0, # index date only
      30, # 1 day after to day 30
      365,
      9999 # Any time prior to any time future
    )
  )
}


runAnalysis = 1:9
minCellCount = 5

exportFolder <- tempfile()
fs::dir_create(exportFolder)

# CDMConnector::example_datasets()
con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir("synthea-covid19-200k")))

cdm <- CDMConnector::cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")

cohortSet <- CDMConnector::readCohortSet(system.file("cohorts3", package = "CDMConnector"))

checkmate::assertClass(cdm, "cdm_reference")
cohortSetColnames <- c("cohort_definition_id", "cohort_name", "cohort", "json", "cohort_name_snakecase")

if (!(is.data.frame(cohortSet) && all(names(cohortSet) == cohortSetColnames))) {
  cli::cli_abort("{.arg cohortSet} needs to be a dataframe created by CDMConnector::readCohortSet()")
}

allAnalyses <- c(
  "InclusionStatistics",
  "IncludedSourceConcepts",
  "OrphanConcepts",
  "TimeSeries",
  "VisitContext",
  "BreakdownIndexEvents",
  "IncidenceRate",
  "CohortRelationship",
  "TemporalCohortCharacterization")

selectedAnalyses <- allAnalyses[runAnalysis]

# table names. these are tables created just for running cohort diagnostics
prefix <- paste0("tmp", as.integer(Sys.time()) %% 10000, "_")
conceptCountsTable <- paste0(prefix, "concept_counts")
cohortTableNameWithoutPrefix <- paste0("cohort", as.integer(Sys.time()) %% 10000)
cohortTableName <- paste0(prefix, cohortTableNameWithoutPrefix)

cdm <- CDMConnector::generateCohortSet(
  cdm,
  cohortSet = cohortSet,
  name = cohortTableName,
  computeAttrition = TRUE,
  overwrite = TRUE
)

cohortSet$sql <- character(nrow(cohortSet))

cohortDefinitionSet <- cohortSet %>% 
  dplyr::mutate(
    cohortName = cohort_name, 
    sql = "",
    json = as.character(json),
    cohortId = as.numeric(cohort_definition_id),
    isSubset = FALSE)

# fill in the sql column
for (i in seq_len(nrow(cohortDefinitionSet))) {
  cohortJson <- cohortDefinitionSet$json[[i]]
  cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                        options = CirceR::createGenerateOptions(generateStats = TRUE))
  cohortDefinitionSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
}

cohortTableNames <- list(
  cohortTable = cohortTableName,
  cohortInclusionTable = paste0(cohortTableName, "_inclusion"), 
  cohortInclusionResultTable = paste0(cohortTableName, "_inclusion_result"), 
  cohortInclusionStatsTable = paste0(cohortTableName, "_inclusion_stats"), 
  cohortSummaryStatsTable = paste0(cohortTableName, "_summary_stats"), 
  cohortCensorStatsTable = paste0(cohortTableName, "_censor_stats")
)


#### default params ---
conceptCountsTable = "#concept_counts"
cohortIds = NULL
cdmVersion = 5
temporalCovariateSettings = getDefaultCovariateSettings()
minCellCount = 5
minCharacterizationMean = 0.01
irWashoutPeriod = 0
incremental = FALSE
incrementalFolder = file.path(exportFolder, "incremental")
useExternalConceptCountsTable = FALSE
runOnSample = FALSE
sampleN = 1000
seed = 64374
seedArgs = NULL
sampleIdentifierExpression = "cohortId * 1000 + seed"

########## -------

# cohortDefinitionSet,
databaseName = NULL
databaseDescription = NULL
connectionDetails = NULL
connection = attr(cdm, "dbcon")
cdmVersion = floor(as.numeric(CDMConnector::version(cdm)))
cohortTable = cohortTableName
cohortTableNames = cohortTableNames
conceptCountsTable = paste(attr(cdm, "write_schema"), paste0(prefix, "concept_counts"), sep = ".")
cohortDatabaseSchema = attr(cdm, "write_schema")
cdmDatabaseSchema = attr(cdm, "cdm_schema")
vocabularyDatabaseSchema = cdmDatabaseSchema
exportFolder = exportFolder
databaseId = attr(cdm, "cdm_name")
tempEmulationSchema = attr(cdm, "write_schema")
minCellCount = minCellCount
runInclusionStatistics = "InclusionStatistics" %in% selectedAnalyses
runIncludedSourceConcepts = "IncludedSourceConcepts" %in% selectedAnalyses
runOrphanConcepts = "IncludedSourceConcepts" %in% selectedAnalyses
runTimeSeries = "IncludedSourceConcepts" %in% selectedAnalyses
runVisitContext = "VisitContext" %in% selectedAnalyses
runBreakdownIndexEvents = "BreakdownIndexEvents" %in% selectedAnalyses
runIncidenceRate = "IncidenceRate" %in% selectedAnalyses
runCohortRelationship = "CohortRelationship" %in% selectedAnalyses
runTemporalCohortCharacterization = "TemporalCohortCharacterization" %in% selectedAnalyses
useExternalConceptCountsTable = FALSE

cohorts = cohortDefinitionSet

DBI::dbListTables(con)

# DBI::dbDisconnect(con, shutdown = T)





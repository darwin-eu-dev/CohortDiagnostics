# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Run Diagnostics for a cohort set
#' 
#' This is the main function exported by the CohortDiagnositcs package. It runs the 
#' diagnostic analysis and saves the results to a folder.
#' 
#'
#' @param cdm A cdm_reference object create by CDMConnector::cdm_from_con()
#' @param cohortSet A cohort set created by CDMConnector::readCohortSet()
#' @param exportFolder The folder where the output will be exported to. If this folder does not exist it will be created.
#' @param runAnalysis A numeric vector corresponding to the diagnostic analyses should be executed. See Details
#' @param minCellCount  The minimum cell count for fields contains person counts or fractions.
#' 
#' @details runAnalysis argument
#' 
#' Pass a numeric vector matching the following analyses. Negative numbers will be interpreted as exclusions.
#' Use a numeric vector to select the analyses you want to run. Negative numbers will exclude analyses.
#' The selection works the same way as vector subsetting in R.
#' 
#' # Analyses to choose from:
#' 1. InclusionStatistics - TODO fill in descriptions of each analyses
#' 1. IncludedSourceConcepts - 
#' 1. OrphanConcepts - Find potential orphan concepts that should possibly included in your concept set.
#' 1. TimeSeries -
#' 1. VisitContext - 
#' 1. BreakdownIndexEvents
#' 1. IncidenceRate
#' 1. CohortRelationship
#' 1. TemporalCohortCharacterization
#' 
#' @md
#' 
#' @examples
#' \dontrun{
#' library(CDMConnector)
#' con <- DBI::dbConnect(duckdb::duckdb(eunomia_dir()))
#' 
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
#' 
#' cohortSet <- CDMConnector::readCohortSet(system.file("cohorts", package = "CohortDiagnostics"))
#' 
#' executeDiagnosticsCdm(
#'  cdm,
# ' cohortSet,
# ' exportFolder,
# ' runAnalysis = 1:9,
# ' minCellCount = 5) 
#' }
#' 
#' @export
executeDiagnosticsCdm <- function(cdm,
                                  cohortSet,
                                  exportFolder,
                                  runAnalysis = 1:9,
                                  minCellCount = 5) { 
  
  checkmate::assertClass(cdm, "cdm_reference")
  checkmate::assertClass(cohortSet, "data.frame")
  exportFolder <- normalizePath(exportFolder, mustWork = TRUE)
  fs::dir_create(exportFolder)
  checkmate::assert_path_for_output(exportFolder, overwrite = TRUE)
  checkmate::assertIntegerish(minCellCount, lower = 0, len = 1, any.missing = FALSE)
  
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
  conceptCountsTable = paste0(prefix, "concept_counts")
  cohortTable = paste0(prefix, "cohort")
  
  
  cdm <- CDMConnector::generateCohortSet(
    cdm,
    cohortSet = cohortSet,
    name = cohortTable,
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
  
  # fill in the sql column with circe SQL
  for (i in seq_len(nrow(cohortDefinitionSet))) {
    cohortJson <- cohortDefinitionSet$json[[i]]
    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                          options = CirceR::createGenerateOptions(generateStats = TRUE))
    cohortDefinitionSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
  }
  
  # Parameters -----
  # cohortDefinitionSet
  # exportFolder 
  databaseId <- cdmName(cdm)
  cohortDatabaseSchema <- attr(cdm, "write_schema")
  databaseName = cdmName(cdm)
  databaseDescription = NULL 
  connectionDetails = NULL 
  connection = attr(cdm, "dbcon")
  cdmVersion = floor(as.numeric(CDMConnector::version(cdm)))
  cohortTable = paste0(attr(cdm, "write_prefix"), "cd_cohort")
  conceptCountsTable <- paste0(attr(cdm, "write_prefix"), "cd_concept_counts")
  cdmDatabaseSchema <- attr(cdm, "cdm_schema")
  tempEmulationSchema = attr(cdm, "write_schema")
  cohortTable = "cohort"
  cohortTableNames = CohortGenerator:getCohortTableNames(cohortTable = cohortTable)
  conceptCountsTable = "#concept_counts"
  vocabularyDatabaseSchema = cdmDatabaseSchema
  cohortIds = NULL
  cdmVersion = 5
  runInclusionStatistics = "InclusionStatistics" %in% selectedAnalyses
  runIncludedSourceConcepts = "IncludedSourceConcepts" %in% selectedAnalyses
  runOrphanConcepts = "IncludedSourceConcepts" %in% selectedAnalyses
  runTimeSeries = "IncludedSourceConcepts" %in% selectedAnalyses
  runVisitContext = "VisitContext" %in% selectedAnalyses
  runBreakdownIndexEvents = "BreakdownIndexEvents" %in% selectedAnalyses
  runIncidenceRate = "IncidenceRate" %in% selectedAnalyses
  runCohortRelationship = "CohortRelationship" %in% selectedAnalyses
  runTemporalCohortCharacterization = "TemporalCohortCharacterization" %in% selectedAnalyses
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
  
  # Input argument checking
  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)
  executionTimePath <- file.path(exportFolder, "taskExecutionTimes.csv")
  
  start <- Sys.time()
  ParallelLogger::logInfo("Run Cohort Diagnostics started at ", start)
  
  databaseId <- as.character(databaseId)
  
  if (any(is.null(databaseName), is.na(databaseName))) {
    ParallelLogger::logTrace(" - Databasename was not provided. Using CDM source table")
  }
  
  if (any(is.null(databaseDescription), is.na(databaseDescription))) {
    ParallelLogger::logTrace(" - Database description was not provided. Using CDM source table")
  }
  
  checkmate::assertList(cohortTableNames, null.ok = FALSE, types = "character", names = "named")
  checkmate::assertNames(names(cohortTableNames),
                         must.include = c(
                           "cohortTable",
                           "cohortInclusionTable",
                           "cohortInclusionResultTable",
                           "cohortInclusionStatsTable",
                           "cohortSummaryStatsTable",
                           "cohortCensorStatsTable"
                         ),
                         add = errorMessage
  )
  checkmate::assertDataFrame(cohortDefinitionSet)
  checkmate::assertNames(names(cohortDefinitionSet), must.include = c("json","cohortId", "cohortName"))
  
  cohortTable <- cohortTableNames$cohortTable
  
  checkmate::assertInt(cdmVersion, lower = 5, upper = 5)
  checkmate::assertIntegerish(x = minCellCount, len = 1, lower = 0)
  checkmate::assertNumeric(x = minCharacterizationMean, lower = 0)
  checkmate::assertLogical(incremental)
  checkmate::assertCharacter(cdmDatabaseSchema, min.len = 1, any.missing = FALSE)
  checkmate::assertCharacter(vocabularyDatabaseSchema, min.len = 1, any.missing = FALSE)
  checkmate::assertCharacter(cohortDatabaseSchema, min.len = 1, any.missing = FALSE)
  checkmate::assertCharacter(cohortTable, min.len = 1, any.missing = FALSE)
  checkmate::assertCharacter(databaseId, min.len = 1, any.missing = FALSE)
  
  ParallelLogger::addDefaultFileLogger(file.path(exportFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(exportFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )
  
  if (incremental) {
    fs::dir_create(incrementalFolder)
    checkmate::assertDirectoryExists(incrementalFolder)
  }
  
  if (runTemporalCohortCharacterization) {
    if (is(temporalCovariateSettings, "covariateSettings")) {
      temporalCovariateSettings <- list(temporalCovariateSettings)
    }
    
    # All temporal covariate settings objects must be covariateSettings
    checkmate::assert_true(all(lapply(temporalCovariateSettings, class) == c("covariateSettings")))
    
    requiredCharacterisationSettings <- c(
      "DemographicsGender", "DemographicsAgeGroup", "DemographicsRace",
      "DemographicsEthnicity", "DemographicsIndexYear", "DemographicsIndexMonth",
      "ConditionEraGroupOverlap", "DrugEraGroupOverlap", "CharlsonIndex",
      "Chads2", "Chads2Vasc"
    )
    presentSettings <- temporalCovariateSettings[[1]][requiredCharacterisationSettings]
    if (!all(unlist(presentSettings))) {
      warning(
        "For cohort charcterization to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
        paste(requiredCharacterisationSettings, collapse = ", ")
      )
    }
    
    requiredTimeDistributionSettings <- c(
      "DemographicsPriorObservationTime",
      "DemographicsPostObservationTime",
      "DemographicsTimeInCohort"
    )
    
    presentSettings <- temporalCovariateSettings[[1]][requiredTimeDistributionSettings]
    if (!all(unlist(presentSettings))) {
      warning(
        "For time distributions diagnostics to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
        paste(requiredTimeDistributionSettings, collapse = ", ")
      )
    }
    
    # forcefully set ConditionEraGroupStart and drugEraGroupStart to NULL
    # because of known bug in FeatureExtraction. https://github.com/OHDSI/FeatureExtraction/issues/144
    temporalCovariateSettings[[1]]$ConditionEraGroupStart <- NULL
    temporalCovariateSettings[[1]]$DrugEraGroupStart <- NULL
    
    checkmate::assert_integerish(
      x = temporalCovariateSettings[[1]]$temporalStartDays,
      any.missing = FALSE,
      min.len = 1,
      add = errorMessage
    )
    checkmate::assert_integerish(
      x = temporalCovariateSettings[[1]]$temporalEndDays,
      any.missing = FALSE,
      min.len = 1,
      add = errorMessage
    )
    checkmate::reportAssertions(collection = errorMessage)
    
    # Adding required temporal windows required in results viewer
    requiredTemporalPairs <-
      list(
        c(-365, 0),
        c(-30, 0),
        c(-365, -31),
        c(-30, -1),
        c(0, 0),
        c(1, 30),
        c(31, 365),
        c(-9999, 9999)
      )
    for (p1 in requiredTemporalPairs) {
      found <- FALSE
      for (i in 1:length(temporalCovariateSettings[[1]]$temporalStartDays)) {
        p2 <- c(
          temporalCovariateSettings[[1]]$temporalStartDays[i],
          temporalCovariateSettings[[1]]$temporalEndDays[i]
        )
        
        if (p2[1] == p1[1] & p2[2] == p1[2]) {
          found <- TRUE
          break
        }
      }
      
      if (!found) {
        temporalCovariateSettings[[1]]$temporalStartDays <-
          c(temporalCovariateSettings[[1]]$temporalStartDays, p1[1])
        temporalCovariateSettings[[1]]$temporalEndDays <-
          c(temporalCovariateSettings[[1]]$temporalEndDays, p1[2])
      }
    }
  }
  
  checkmate::reportAssertions(collection = errorMessage)
  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet %>% dplyr::filter(.data$cohortId %in% cohortIds)
  }
  
  if (nrow(cohortDefinitionSet) == 0) {
    stop("No cohorts specified")
  }
  cohortTableColumnNamesObserved <- colnames(cohortDefinitionSet) %>%
    sort()
  cohortTableColumnNamesExpected <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  cohortTableColumnNamesRequired <-
    getResultsDataModelSpecifications() %>%
    dplyr::filter(.data$tableName == "cohort") %>%
    dplyr::filter(.data$isRequired == "Yes") %>%
    dplyr::pull(.data$columnName) %>%
    SqlRender::snakeCaseToCamelCase() %>%
    sort()
  
  expectedButNotObsevered <-
    setdiff(x = cohortTableColumnNamesExpected, y = cohortTableColumnNamesObserved)
  if (length(expectedButNotObsevered) > 0) {
    requiredButNotObsevered <-
      setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
  }
  obseveredButNotExpected <-
    setdiff(x = cohortTableColumnNamesObserved, y = cohortTableColumnNamesExpected)
  
  if (length(requiredButNotObsevered) > 0) {
    stop(paste(
      "The following required fields not found in cohort table:",
      paste0(requiredButNotObsevered, collapse = ", ")
    ))
  }
  
  if (length(obseveredButNotExpected) > 0) {
    ParallelLogger::logInfo(
      paste0(
        "The following fields found in the cohortDefinitionSet will be exported in JSON format as part of metadata field of cohort table:\n    ",
        paste0(obseveredButNotExpected, collapse = ",\n    ")
      )
    )
  }
  
  cohortDefinitionSet <- makeDataExportable(
    x = cohortDefinitionSet,
    tableName = "cohort",
    minCellCount = minCellCount,
    databaseId = NULL
  )
  
  writeToCsv(
    data = cohortDefinitionSet,
    fileName = file.path(exportFolder, "cohort.csv")
  )
  
  subsets <- CohortGenerator::getSubsetDefinitions(cohortDefinitionSet)
  if (length(subsets)) {
    dfs <- lapply(subsets, function(x) {
      data.frame(subsetDefinitionId = x$definitionId, json = as.character(x$toJSON()))
    })
    subsetDefinitions <- data.frame()
    for (subsetDef in dfs) {
      subsetDefinitions <- rbind(subsetDefinitions, subsetDef)
    }
    
    writeToCsv(
      data = subsetDefinitions,
      fileName = file.path(exportFolder, "subset_definition.csv")
    )
  }
  
  
  
}

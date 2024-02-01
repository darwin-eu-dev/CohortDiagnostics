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
#' 1. InclusionStatistics - Report the number of subjects that match specific inclusion rules in each cohort definition.
#' 2. IncludedSourceConcepts - Report the source concepts observed in the database that are included in a concept set of a cohort. 
#' 3. OrphanConcepts - Report the source concepts observed in the database that are not included in a concept set of a cohort, but maybe should be. 
#' 4. TimeSeries - Boxplot and a table showing the distribution of time (in days) before and after the cohort index date (cohort start date), and the time between cohort start and end date.
#' 5. VisitContext - Report the relationship between the cohort start date and visits recorded in the database. 
#' 6. BreakdownIndexEvents - Report the concepts belonging to the concept sets in the entry event definition that are observed on the index date.
#' 7. IncidenceRate - A graph showing the incidence rate, optionally stratified by age (in 10-year bins), gender, and calendar year.
#' 8. CohortRelationship - Stacked bar graph showing the overlap between two cohorts, and a table listing several overlap statistics.
#' 9. TemporalCohortCharacterization - A table showing temporal cohort characteristics (covariates). These characteristics are captured at specific time intervals before or after cohort start date. 
#' 
#' @md
#' 
#' @examples
#' \dontrun{
#' 
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' 
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
#' 
#' cohortDefinitionSet <- CDMConnector::readCohortSet(system.file("cohorts", package = "CohortDiagnostics"))
#' 
#' # run all analyses
#' executeDiagnosticsCdm(
#'  cdm,
# ' cohortSet,
# ' exportFolder,
# ' runAnalysis = 1:9,
# ' minCellCount = 5) 
#' 
#' # skip CohortRelationship and TemporalCohortCharacterization
#' executeDiagnosticsCdm(
#'  cdm,
# ' cohortSet,
# ' exportFolder,
# ' runAnalysis = c(-8, -9),
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
  
  # fill in the sql column
  for (i in seq_len(nrow(cohortDefinitionSet))) {
    cohortJson <- cohortDefinitionSet$json[[i]]
    cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
    cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                          options = CirceR::createGenerateOptions(generateStats = TRUE))
    cohortDefinitionSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
  }
  
  executeDiagnostics(cohortDefinitionSet,
                     connectionDetails = NULL,
                     connection = attr(cdm, "dbcon"),
                     cdmVersion = floor(as.numeric(CDMConnector::version(cdm))),
                     cohortTable = cohortTable,
                     conceptCountsTable = conceptCountsTable,
                     # cohortTable = paste(attr(cdm, "write_schema"), cohortTable, sep = "."),
                     # conceptCountsTable = paste(attr(cdm, "write_schema"), conceptCountsTable, sep = "."),
                     cohortDatabaseSchema = attr(cdm, "write_schema"),
                     cdmDatabaseSchema = attr(cdm, "cdm_schema"),
                     exportFolder = exportFolder,
                     databaseId = attr(cdm, "cdm_name"),
                     tempEmulationSchema = attr(cdm, "write_schema"),
                     minCellCount = minCellCount,
                     runInclusionStatistics = "InclusionStatistics" %in% selectedAnalyses,
                     runIncludedSourceConcepts = "IncludedSourceConcepts" %in% selectedAnalyses,
                     runOrphanConcepts = "IncludedSourceConcepts" %in% selectedAnalyses,
                     runTimeSeries = "IncludedSourceConcepts" %in% selectedAnalyses,
                     runVisitContext = "VisitContext" %in% selectedAnalyses,
                     runBreakdownIndexEvents = "BreakdownIndexEvents" %in% selectedAnalyses,
                     runIncidenceRate = "IncidenceRate" %in% selectedAnalyses,
                     runCohortRelationship = "CohortRelationship" %in% selectedAnalyses,
                     runTemporalCohortCharacterization = "TemporalCohortCharacterization" %in% selectedAnalyses,
                     useExternalConceptCountsTable = FALSE)
  
}

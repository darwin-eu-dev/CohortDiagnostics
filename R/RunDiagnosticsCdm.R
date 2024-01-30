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
#' 
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' 
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
#' 
#' cohortDefinitionSet <- CDMConnector::readCohortSet(system.file("cohorts", package = "CohortDiagnostics"))
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
  
  cohortDefinitionSet <- cohortSet %>% 
    dplyr::mutate(
      cohortName = cohort_name, 
      sql = "adsf",
      json = as.character(json),
      cohortId = as.numeric(cohort_definition_id))
  
  executeDiagnostics(cohortDefinitionSet,
                     connectionDetails = NULL,
                     connection = attr(cdm, "dbcon"),
                     cdmVersion = floor(as.numeric(CDMConnector::version(cdm))),
                     cohortTable = cohortTable,
                     conceptCountsTable = conceptCountsTable,
                     cohortDatabaseSchema = attr(cdm, "write_schema"),
                     cdmDatabaseSchema = attr(cdm, "cdm_schema"),
                     exportFolder = exportFolder,
                     databaseId = attr(cdm, "cdm_name"),
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

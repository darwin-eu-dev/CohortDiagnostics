% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/RunDiagnostics.R
\name{runCohortDiagnostics}
\alias{runCohortDiagnostics}
\title{Run cohort diagnostics}
\usage{
runCohortDiagnostics(
  packageName = NULL,
  cohortToCreateFile = "settings/CohortsToCreate.csv",
  cohortDefinitionSet = NULL,
  baseUrl = NULL,
  cohortSetReference = NULL,
  connectionDetails = NULL,
  connection = NULL,
  cdmDatabaseSchema,
  oracleTempSchema = NULL,
  tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
  cohortDatabaseSchema,
  vocabularyDatabaseSchema = cdmDatabaseSchema,
  cohortTable = "cohort",
  cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = cohortTable),
  cohortIds = NULL,
  inclusionStatisticsFolder = NULL,
  exportFolder,
  databaseId,
  databaseName = databaseId,
  databaseDescription = databaseId,
  cdmVersion = 5,
  runInclusionStatistics = TRUE,
  runIncludedSourceConcepts = TRUE,
  runOrphanConcepts = TRUE,
  runTimeDistributions = TRUE,
  runVisitContext = TRUE,
  runBreakdownIndexEvents = TRUE,
  runIncidenceRate = TRUE,
  runTimeSeries = FALSE,
  runCohortOverlap = TRUE,
  runCohortCharacterization = TRUE,
  covariateSettings = createDefaultCovariateSettings(),
  runTemporalCohortCharacterization = TRUE,
  temporalCovariateSettings = createTemporalCovariateSettings(useConditionOccurrence =
    TRUE, useDrugEraStart = TRUE, useProcedureOccurrence = TRUE, useMeasurement = TRUE,
    temporalStartDays = c(-365, -30, 0, 1, 31), temporalEndDays = c(-31, -1, 0, 30, 365)),
  minCellCount = 5,
  incremental = FALSE,
  incrementalFolder = file.path(exportFolder, "incremental")
)
}
\arguments{
\item{packageName}{The name of the package containing the cohort definitions. Can be left NULL if 
\code{baseUrl} and \code{cohortSetReference} have been specified.}

\item{cohortToCreateFile}{The location of the cohortToCreate file within the package. Is ignored if 
\code{baseUrl} and \code{cohortSetReference} have been specified. The
cohortToCreateFile must be .csv file that is expected to be read into 
a dataframe object identical to requirements for \code{cohortSetReference} argument. 
This csv file is expected to be encoded in either ASCII or UTF-8, if not, an 
error message will be displayed and process stopped.}

\item{cohortDefinitionSet}{Data.frame of cohorts must include columns cohortId, cohortName, json, sql}

\item{baseUrl}{The base URL for the WebApi instance, for example:
"http://server.org:80/WebAPI". Can be left NULL if 
\code{packageName} and \code{cohortToCreateFile} have been specified.}

\item{cohortSetReference}{A data frame with four columns, as described in the details. Can be left NULL if 
\code{packageName} and \code{cohortToCreateFile} have been specified.}

\item{connectionDetails}{An object of type \code{connectionDetails} as created using the
\code{\link[DatabaseConnector]{createConnectionDetails}} function in the
DatabaseConnector package. Can be left NULL if \code{connection} is
provided.}

\item{connection}{An object of type \code{connection} as created using the
\code{\link[DatabaseConnector]{connect}} function in the
DatabaseConnector package. Can be left NULL if \code{connectionDetails}
is provided, in which case a new connection will be opened at the start
of the function, and closed when the function finishes.}

\item{cdmDatabaseSchema}{Schema name where your patient-level data in OMOP CDM format resides.
Note that for SQL Server, this should include both the database and
schema name, for example 'cdm_data.dbo'.}

\item{oracleTempSchema}{DEPRECATED by DatabaseConnector: use \code{tempEmulationSchema} instead.}

\item{tempEmulationSchema}{Some database platforms like Oracle and Impala do not truly support temp tables. To emulate temp 
tables, provide a schema with write privileges where temp tables can be created.}

\item{cohortDatabaseSchema}{Schema name where your cohort table resides. Note that for SQL Server,
this should include both the database and schema name, for example
'scratch.dbo'.}

\item{vocabularyDatabaseSchema}{Schema name where your OMOP vocabulary data resides. This is 
commonly the same as cdmDatabaseSchema. Note that for 
SQL Server, this should include both the database and
schema name, for example 'vocabulary.dbo'.}

\item{cohortTable}{Name of the cohort table.}

\item{cohortTableNames}{Cohort Table names used by CohortGenerator package}

\item{cohortIds}{Optionally, provide a subset of cohort IDs to restrict the
diagnostics to.}

\item{inclusionStatisticsFolder}{The folder where the inclusion rule statistics are stored. Can be
left NULL if \code{runInclusionStatistics = FALSE}.}

\item{exportFolder}{The folder where the output will be exported to. If this folder
does not exist it will be created.}

\item{databaseId}{A short string for identifying the database (e.g. 'Synpuf').}

\item{databaseName}{The full name of the database. If NULL, defaults to databaseId.}

\item{databaseDescription}{A short description (several sentences) of the database. If NULL, defaults to databaseId.}

\item{cdmVersion}{The version of the OMOP CDM. Default 5. (Note: only 5 is supported.)}

\item{runInclusionStatistics}{Generate and export statistic on the cohort inclusion rules?}

\item{runIncludedSourceConcepts}{Generate and export the source concepts included in the cohorts?}

\item{runOrphanConcepts}{Generate and export potential orphan concepts?}

\item{runTimeDistributions}{Generate and export cohort time distributions?}

\item{runVisitContext}{Generate and export index-date visit context?}

\item{runBreakdownIndexEvents}{Generate and export the breakdown of index events?}

\item{runIncidenceRate}{Generate and export the cohort incidence  rates?}

\item{runTimeSeries}{Generate and export the cohort prevalence  rates?}

\item{runCohortOverlap}{Generate and export the cohort overlap? Overlaps are checked within cohortIds
that have the same phenotype ID sourced from the CohortSetReference or
cohortToCreateFile.}

\item{runCohortCharacterization}{Generate and export the cohort characterization?
Only records with values greater than 0.0001 are returned.}

\item{covariateSettings}{Either an object of type \code{covariateSettings} as created using one of
the createCovariateSettings function in the FeatureExtraction package, or a list
of such objects.}

\item{runTemporalCohortCharacterization}{Generate and export the temporal cohort characterization?
Only records with values greater than 0.001 are returned.}

\item{temporalCovariateSettings}{Either an object of type \code{covariateSettings} as created using one of
the createTemporalCovariateSettings function in the FeatureExtraction package, or a list
of such objects.}

\item{minCellCount}{The minimum cell count for fields contains person counts or fractions.}

\item{incremental}{Create only cohort diagnostics that haven't been created before?}

\item{incrementalFolder}{If \code{incremental = TRUE}, specify a folder where records are kept
of which cohort diagnostics has been executed.}
}
\description{
Runs the cohort diagnostics on all (or a subset of) the cohorts instantiated using the
\code{ROhdsiWebApi::insertCohortDefinitionSetInPackage} function. Assumes the cohorts have already been instantiated.

Characterization:
If runTemporalCohortCharacterization argument is TRUE, then the following default covariateSettings object will be created
using \code{RFeatureExtraction::createTemporalCovariateSettings}
Alternatively, a covariate setting object may be created using the above as an example.
}
\details{
Currently two ways of executing this function are supported, either 
(1) [Package Mode] embedded in a study package, assuming the cohort definitions are stored in that package using the \code{ROhdsiWebApi::insertCohortDefinitionSetInPackage}, or 
(2) [WebApi Mode] By using a WebApi interface to retrieve the cohort definitions.

When using this function in Package Mode: Use the \code{packageName} and \code{cohortToCreateFile} to specify
the name of the study package, and the name of the cohortToCreate file within that package, respectively

When using this function in WebApi Mode: use the \code{baseUrl} and \code{cohortSetReference} to specify how to 
connect to the WebApi, and which cohorts to fetch, respectively.

Note: if the parameters for both Package Mode and WebApi Mode are provided, then Package mode is preferred.

The \code{cohortSetReference} argument must be a data frame with  the following columns:
\describe{
\item{cohortId}{The cohort Id is the id used to identify  a cohort definition. This is required to be unique. It will be used to create file names. It is recommended to be (referrentConceptId * 1000) + a number between 3 to 999}
\item{atlasId}{Cohort Id in the webApi/atlas instance. It is a required field to run Cohort Diagnostics in WebApi mode. It is discarded in package mode.}
\item{cohortName}{The full name of the cohort. This will be shown in the Shiny app.}
\item{logicDescription}{A human understandable brief description of the cohort definition. This logic does not have to a 
fully specified description of the cohort definition, but should provide enough context to help user understand the
meaning of the cohort definition}
\item{referentConceptId}{A standard omop concept id that serves as the referent phenotype definition for the cohort Id (optional)}
}
}

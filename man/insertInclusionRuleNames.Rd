% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/InclusionRules.R
\name{insertInclusionRuleNames}
\alias{insertInclusionRuleNames}
\title{Used to insert the inclusion rule names from a cohort definition set
when generating cohorts that include cohort statistics}
\usage{
insertInclusionRuleNames(
  connection,
  cohortDefinitionSet,
  cohortDatabaseSchema,
  cohortInclusionTable = getCohortTableNames()$cohortInclusionTable
)
}
\arguments{
\item{connection}{db connection}

\item{cohortDefinitionSet}{cohort definition set}

\item{cohortDatabaseSchema}{Schema name where your user has write access (has CRUD privileges). This is the location,
of the cohort tables. This is also the location for the optional table such as 
the unique_concept_id table. 
In incremental mode: It is assumed that the tables in this location have not been modified 
outside this application, and the content was written by this application for current 
project only. A good practice would be to ensure that the database table names are unique 
for the project. e.g. 'EPI101_COHORT', 'EPI101_UNIQUE_CONCEPT_ID' (please try to keep 
length < 22). Another assumption: the content of these tables were written by this application
(i.e. they were not altered outside of this application) and the tables are available during the
life cycle of this project. The local files that are created by the application during incremental
mode such as the 'recordKeepingFile'.}

\item{cohortInclusionTable}{Name of the inclusion table, one of the tables for storing
inclusion rule statistics.}
}
\value{
A data frame containing the inclusion rules by cohort and sequence ID
}
\description{
This function will take a cohortDefinitionSet that inclusions the Circe JSON
representation of each cohort, parse the InclusionRule property to obtain
the inclusion rule name and sequence number and insert the values into the
cohortInclusionTable. This function is only required when generating cohorts
that include cohort statistics.
}

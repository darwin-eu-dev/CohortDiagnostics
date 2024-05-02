#' Create a cohort definition set.
#'
#' @description
#' This method will create a data frame containing the cohort definitions. The data frame contains the
#' cohort id, name, the sql and the json. The sql is generated from the JSON.
#'
#' @param cohortJsonFolder The folder containing the cohort Json files.
#' 
#' @return a data frame containing the cohort definitions
#'
#' @export
createCohortDefinitionSet <- function(cohortJsonFolder) {
  checkmate::assertDirectory(cohortJsonFolder)
  
  cohortDefinitionSet <- CohortGenerator::createEmptyCohortDefinitionSet()
  cohortJsonFiles <- list.files(path = cohortJsonFolder, full.names = TRUE)
  for (i in 1:length(cohortJsonFiles)) {
    cohortJsonFileName <- cohortJsonFiles[i]
    cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
    # Here we read in the JSON in order to create the SQL
    cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
    cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
    cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
    cohortDefinitionSet <- rbind(cohortDefinitionSet, data.frame(cohortId = as.numeric(i),
                                                                 cohortName = cohortName,
                                                                 sql = cohortSql,
                                                                 json = cohortJson,
                                                                 stringsAsFactors = FALSE))
  }
  return(cohortDefinitionSet)
}
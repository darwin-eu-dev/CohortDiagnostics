# Always export concept sets to csv
cli::cli_inform("Exporting cohort concept sets to csv")
# We need to get concept sets from all cohorts in case subsets are present and
# Added incrementally after cohort generation

# cohorts should be a dataframe with at least cohortId, sql and json

conceptSets <- list()
conceptSetCounter <- 0
for (i in  seq_len(nrow(cohorts))) {
  cohort <- cohorts[i, ]
  
  # jsonCs <- extractConceptSetsJsonFromCohortJson(cohortJson = cohort$json)
  
  cohortDefinition <- tryCatch(
    {
      RJSONIO::fromJSON(content = cohortJson, digits = 23)
    },
    error = function(msg) {
      return(list())
    }
  )
  if ("expression" %in% names(cohortDefinition)) {
    expression <- cohortDefinition$expression
  } else {
    expression <- cohortDefinition
  }
  conceptSetExpression <- list()
  if (length(expression$ConceptSets) > 0) {
    for (i in (1:length(expression$ConceptSets))) {
      conceptSetExpression[[i]] <-
        dplyr::tibble(
          conceptSetId = expression$ConceptSets[[i]]$id,
          conceptSetName = expression$ConceptSets[[i]]$name,
          conceptSetExpression = expression$ConceptSets[[i]]$expression$items %>% RJSONIO::toJSON(digits = 23)
        )
    }
  } else {
    conceptSetExpression <- dplyr::tibble()
  }
  
  if (nrow(sqlCs) == 0 || nrow(jsonCs) == 0) {
    ParallelLogger::logInfo(
      "Cohort Definition expression does not have a concept set expression. ",
      "Skipping Cohort: ",
      cohort$cohortName
    )
  } else {
    if (length(sqlCs) > 0 && length(jsonCs) > 0) {
      conceptSetCounter <- conceptSetCounter + 1
      conceptSets[[conceptSetCounter]] <-
        dplyr::tibble(
          cohortId = cohort$cohortId,
          dplyr::inner_join(x = sqlCs %>% dplyr::distinct(), y = jsonCs %>% dplyr::distinct(), by = "conceptSetId")
        )
    }
  }
}

conceptSets <- dplyr::bind_rows(conceptSets) %>%
  dplyr::arrange(.data$cohortId, .data$conceptSetId)

uniqueConceptSets <- conceptSets %>%
  dplyr::select("conceptSetExpression") %>%
  dplyr::mutate(uniqueConceptSetId = dplyr::row_number()) %>%
  dplyr::distinct()

conceptSets <- conceptSets %>%
  dplyr::inner_join(uniqueConceptSets,
                    by = "conceptSetExpression",
                    relationship = "many-to-many"
  ) %>%
  dplyr::distinct() %>%
  dplyr::relocate(
    "uniqueConceptSetId",
    "cohortId",
    "conceptSetId"
  ) %>%
  dplyr::arrange(
    .data$uniqueConceptSetId,
    .data$cohortId,
    .data$conceptSetId
  )
return(conceptSets)

conceptSets <- conceptSets %>%
  dplyr::select(-"uniqueConceptSetId") %>%
  dplyr::distinct()
# Save concept set metadata ---------------------------------------
conceptSetsExport <- makeDataExportable(
  x = conceptSets,
  tableName = "concept_sets",
  minCellCount = minCellCount,
  databaseId = databaseId
)

# Always write all concept sets for all cohorts as they are always needed
writeToCsv(
  data = conceptSetsExport,
  fileName = file.path(exportFolder, "concept_sets.csv"),
  incremental = FALSE,
  cohortId = conceptSetsExport$cohortId
)
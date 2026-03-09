library(testthat)

makeConceptSetJson <- function(ids = c(1, 2), wrapExpression = TRUE) {
  conceptSets <- lapply(ids, function(id) {
    list(
      id = id,
      name = paste0("set_", id),
      expression = list(items = list(list(conceptId = 1000 + id)))
    )
  })
  payload <- list(ConceptSets = conceptSets)
  if (wrapExpression) {
    payload <- list(expression = payload)
  }
  jsonlite::toJSON(payload, auto_unbox = TRUE)
}

makeConceptSetSql <- function(ids = c(1, 2)) {
  sqlBlocks <- vapply(ids, function(id) {
    paste0(
      "SELECT ", id, " as codeset_id, c.concept_id FROM ",
      "(SELECT concept_id FROM concept WHERE concept_id IN (", 1000 + id, ")) C"
    )
  }, FUN.VALUE = character(1))
  paste(c(sqlBlocks, "with primary_events as (select 1)"), collapse = "\nUNION ALL\n")
}

test_that("extractConceptSetsSqlFromCohortSql handles empty and parses concept set ids", {
  expect_error(
    CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(c("a", "b")),
    "more than one cohort SQL"
  )

  empty <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql("")
  expect_equal(nrow(empty), 0)

  parsed <- CohortDiagnostics:::extractConceptSetsSqlFromCohortSql(makeConceptSetSql(c(1, 2)))
  expect_equal(parsed$conceptSetId, c(1, 2))
  expect_true(all(grepl("SELECT [0-9]+ as codeset_id", parsed$conceptSetSql)))
})

test_that("extractConceptSetsJsonFromCohortJson supports wrapped and unwrapped expression", {
  invalid <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson("not-json")
  expect_equal(nrow(invalid), 0)

  wrapped <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(makeConceptSetJson(c(1, 2), TRUE))
  expect_equal(sort(wrapped$conceptSetId), c(1, 2))
  expect_true(all(c("conceptSetName", "conceptSetExpression") %in% names(wrapped)))

  unwrapped <- CohortDiagnostics:::extractConceptSetsJsonFromCohortJson(makeConceptSetJson(c(3), FALSE))
  expect_equal(unwrapped$conceptSetId, 3)
})

test_that("getParentCohort resolves recursively and combines concept sets across subsets", {
  cohorts <- dplyr::tibble(
    cohortId = c(101, 102),
    cohortName = c("base", "subset"),
    cohortFullName = c("base", "subset"),
    sql = c(makeConceptSetSql(c(1, 2)), "SELECT 999 as codeset_id FROM (SELECT 999) C"),
    json = c(makeConceptSetJson(c(1, 2), TRUE), makeConceptSetJson(c(999), TRUE)),
    isSubset = c(FALSE, TRUE),
    subsetParent = c(NA_integer_, 101L)
  )

  parent <- CohortDiagnostics:::getParentCohort(cohorts[2, ], cohorts)
  expect_equal(parent$cohortId, 101)

  combined <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  expect_equal(nrow(combined), 4)
  expect_equal(sort(unique(combined$cohortId)), c(101, 102))
  expect_equal(length(unique(combined$uniqueConceptSetId)), 2)
})

test_that("combineConceptSetsFromCohorts errors when SQL and JSON concept ids mismatch", {
  mismatched <- dplyr::tibble(
    cohortId = 201,
    cohortName = "mismatch",
    cohortFullName = "mismatch",
    sql = makeConceptSetSql(c(1, 2)),
    json = makeConceptSetJson(c(1), TRUE),
    isSubset = FALSE,
    subsetParent = NA_integer_
  )

  expect_error(
    CohortDiagnostics:::combineConceptSetsFromCohorts(mismatched),
    "Mismatch in concept set IDs"
  )
})

test_that("getCodeSetId/getCodeSetIds and exportConceptSets behave as expected", {
  listCriterion <- list(CodesetId = 123)
  vecCriterion <- c(CodesetId = 234)
  expect_equal(CohortDiagnostics:::getCodeSetId(listCriterion), 123)
  expect_equal(CohortDiagnostics:::getCodeSetId(vecCriterion), 234)
  expect_null(CohortDiagnostics:::getCodeSetId(1))

  codeSetIds <- CohortDiagnostics:::getCodeSetIds(list(
    ConditionOccurrence = list(CodesetId = 1),
    DrugExposure = c(CodesetId = 2),
    EmptyDomain = list()
  ))
  expect_equal(sort(codeSetIds$codeSetIds), c(1, 2))

  exportFolder <- tempfile("cd-concept-sets-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  cohortDefinitionSet <- dplyr::tibble(
    cohortId = 301,
    cohortName = "cohort_export",
    cohortFullName = "cohort_export",
    sql = makeConceptSetSql(c(1, 2)),
    json = makeConceptSetJson(c(1, 2), TRUE),
    isSubset = FALSE,
    subsetParent = NA_integer_
  )

  CohortDiagnostics:::exportConceptSets(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = exportFolder,
    minCellCount = 5,
    databaseId = "db_1"
  )

  filePath <- file.path(exportFolder, "concept_sets.csv")
  expect_true(file.exists(filePath))
  exported <- readr::read_csv(filePath, col_types = readr::cols())
  expect_true(all(c("cohort_id", "concept_set_id", "concept_set_name") %in% names(exported)))
})

test_that("exportConceptSets exits cleanly when no concept sets are available", {
  local_mocked_bindings(
    combineConceptSetsFromCohorts = function(...) {
      data.frame()
    },
    .package = "CohortDiagnostics"
  )

  exportFolder <- tempfile("cd-empty-concept-")
  dir.create(exportFolder)
  on.exit(unlink(exportFolder, recursive = TRUE, force = TRUE), add = TRUE)

  result <- CohortDiagnostics:::exportConceptSets(
    cohortDefinitionSet = dplyr::tibble(),
    exportFolder = exportFolder,
    minCellCount = 5,
    databaseId = "db_1"
  )
  expect_null(result)
  expect_false(file.exists(file.path(exportFolder, "concept_sets.csv")))
})

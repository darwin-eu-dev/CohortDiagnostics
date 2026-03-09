library(testthat)

test_that("makeDataExportable keeps schema for empty cohort_count outputs", {
  emptyCounts <- dplyr::tibble(
    cohortDefinitionId = integer(),
    cohortEntries = integer(),
    cohortSubjects = integer(),
    ignoredField = character()
  )

  formatted <- CohortDiagnostics:::makeDataExportable(
    x = emptyCounts,
    tableName = "cohort_count",
    databaseId = "test_db"
  )

  expect_equal(nrow(formatted), 0)
  expect_true(all(c("cohortId", "cohortEntries", "cohortSubjects", "databaseId") %in% names(formatted)))
  expect_false("ignoredField" %in% names(formatted))
})

test_that("writeToCsv supports zero-row outputs with valid headers", {
  fileName <- tempfile(fileext = ".csv")
  on.exit(unlink(fileName, force = TRUE), add = TRUE)

  emptyCounts <- dplyr::tibble(
    cohortId = integer(),
    cohortEntries = integer(),
    cohortSubjects = integer(),
    databaseId = character()
  )

  CohortDiagnostics:::writeToCsv(
    data = emptyCounts,
    fileName = fileName,
    incremental = FALSE
  )

  expect_true(file.exists(fileName))

  readBack <- readr::read_csv(fileName, col_types = readr::cols())
  expect_equal(nrow(readBack), 0)
  expect_true(all(c("cohort_id", "cohort_entries", "cohort_subjects", "database_id") %in% names(readBack)))
})

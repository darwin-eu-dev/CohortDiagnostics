library(testthat)

test_that("hasData handles null, empty and non-empty inputs", {
  expect_false(CohortDiagnostics:::hasData(NULL))
  expect_false(CohortDiagnostics:::hasData(data.frame()))
  expect_false(CohortDiagnostics:::hasData(numeric(0)))
  expect_false(CohortDiagnostics:::hasData(NA))

  expect_true(CohortDiagnostics:::hasData(data.frame(x = 1)))
  expect_true(CohortDiagnostics:::hasData(c(1, 2, 3)))
})

test_that("computeChecksum is deterministic", {
  x <- c("alpha", "beta", "alpha")
  checksums <- CohortDiagnostics:::computeChecksum(x)

  expect_length(checksums, 3)
  expect_equal(checksums[[1]], checksums[[3]])
  expect_false(checksums[[1]] == checksums[[2]])
})

test_that("getKeyIndex returns expected indexes", {
  recordKeeping <- data.frame(
    cohortId = c(1, 2),
    task = c("a", "b")
  )

  idx <- CohortDiagnostics:::getKeyIndex(
    key = list(cohortId = 2, task = "b"),
    recordKeeping = recordKeeping
  )
  expect_equal(idx, 2)

  idxMissing <- CohortDiagnostics:::getKeyIndex(
    key = list(notAColumn = 1),
    recordKeeping = recordKeeping
  )
  expect_length(idxMissing, 0)
})

test_that("task recordkeeping roundtrip works", {
  recordKeepingFile <- tempfile(fileext = ".csv")
  on.exit(unlink(recordKeepingFile, force = TRUE), add = TRUE)

  # Non-incremental mode should be a no-op
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "demo",
    checksum = "abc",
    recordKeepingFile = recordKeepingFile,
    incremental = FALSE
  )
  expect_false(file.exists(recordKeepingFile))

  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "demo",
    checksum = "abc",
    recordKeepingFile = recordKeepingFile,
    incremental = TRUE
  )

  expect_true(file.exists(recordKeepingFile))
  expect_false(CohortDiagnostics:::isTaskRequired(
    cohortId = 1,
    task = "demo",
    checksum = "abc",
    recordKeepingFile = recordKeepingFile,
    verbose = FALSE
  ))
  expect_true(CohortDiagnostics:::isTaskRequired(
    cohortId = 1,
    task = "demo",
    checksum = "def",
    recordKeepingFile = recordKeepingFile,
    verbose = FALSE
  ))

  # Update existing key with new checksum
  CohortDiagnostics:::recordTasksDone(
    cohortId = 1,
    task = "demo",
    checksum = "def",
    recordKeepingFile = recordKeepingFile,
    incremental = TRUE
  )

  recorded <- readr::read_csv(recordKeepingFile, col_types = readr::cols())
  expect_equal(nrow(recorded), 1)
  expect_equal(recorded$checksum[[1]], "def")
})

test_that("isTaskRequired errors on duplicate keys", {
  recordKeepingFile <- tempfile(fileext = ".csv")
  on.exit(unlink(recordKeepingFile, force = TRUE), add = TRUE)

  duplicateRecords <- data.frame(
    cohortId = c(1, 1),
    task = c("a", "a"),
    checksum = c("x", "x")
  )
  readr::write_csv(duplicateRecords, recordKeepingFile)

  expect_error(
    CohortDiagnostics:::isTaskRequired(
      cohortId = 1,
      task = "a",
      checksum = "x",
      recordKeepingFile = recordKeepingFile,
      verbose = FALSE
    ),
    "Duplicate key"
  )
})

test_that("getRequiredTasks and subset helpers keep only required rows", {
  recordKeepingFile <- tempfile(fileext = ".csv")
  on.exit(unlink(recordKeepingFile, force = TRUE), add = TRUE)

  readr::write_csv(
    data.frame(
      cohortId = c(1, 2),
      task = c("job", "job"),
      checksum = c("a", "b"),
      timeStamp = as.character(Sys.time())
    ),
    recordKeepingFile
  )

  remaining <- CohortDiagnostics:::getRequiredTasks(
    cohortId = c(1, 2, 3),
    task = c("job", "job", "job"),
    checksum = c("a", "b", "c"),
    recordKeepingFile = recordKeepingFile
  )

  expect_equal(nrow(remaining), 1)
  expect_equal(remaining$cohortId[[1]], 3)

  cohorts <- data.frame(
    cohortId = c(1, 2, 3),
    checksum = c("a", "b", "c")
  )

  requiredCohorts <- CohortDiagnostics:::subsetToRequiredCohorts(
    cohorts = cohorts,
    task = "job",
    incremental = TRUE,
    recordKeepingFile = recordKeepingFile
  )

  expect_equal(requiredCohorts$cohortId, 3)

  noSubset <- CohortDiagnostics:::subsetToRequiredCohorts(
    cohorts = cohorts,
    task = "job",
    incremental = FALSE,
    recordKeepingFile = recordKeepingFile
  )
  expect_equal(nrow(noSubset), 3)
})

test_that("subsetToRequiredCombis excludes already processed combinations", {
  recordKeepingFile <- tempfile(fileext = ".csv")
  on.exit(unlink(recordKeepingFile, force = TRUE), add = TRUE)

  combis <- data.frame(
    targetCohortId = c(1, 1, 2),
    comparatorCohortId = c(2, 3, 3),
    targetChecksum = c("ta", "ta", "tb"),
    comparatorChecksum = c("ca", "cb", "cb"),
    checksum = c("x", "y", "z")
  )

  readr::write_csv(
    data.frame(
      cohortId = 1,
      comparatorId = 2,
      targetChecksum = "ta",
      comparatorChecksum = "ca",
      task = "compare",
      checksum = "x",
      timeStamp = as.character(Sys.time())
    ),
    recordKeepingFile
  )

  required <- CohortDiagnostics:::subsetToRequiredCombis(
    combis = combis,
    task = "compare",
    incremental = TRUE,
    recordKeepingFile = recordKeepingFile
  )

  expect_equal(nrow(required), 2)
  expect_true(all(!(required$targetCohortId == 1 & required$comparatorCohortId == 2)))
})

test_that("saveIncremental merges and replaces matching keys", {
  fileName <- tempfile(fileext = ".csv")
  on.exit(unlink(fileName, force = TRUE), add = TRUE)

  readr::write_csv(data.frame(cohort_id = 1, value = 10), fileName)

  CohortDiagnostics:::saveIncremental(
    data = data.frame(cohort_id = 1, value = 20),
    fileName = fileName,
    cohort_id = 1
  )

  replaced <- readr::read_csv(fileName, col_types = readr::cols())
  expect_equal(nrow(replaced), 1)
  expect_equal(replaced$value[[1]], 20)

  CohortDiagnostics:::saveIncremental(
    data = data.frame(cohort_id = 2, value = 30),
    fileName = fileName,
    cohort_id = 2
  )

  appended <- readr::read_csv(fileName, col_types = readr::cols())
  expect_equal(nrow(appended), 2)

  # Empty keys should no-op
  CohortDiagnostics:::saveIncremental(
    data = data.frame(cohort_id = 99, value = 99),
    fileName = fileName,
    cohort_id = integer(0)
  )
  afterNoOp <- readr::read_csv(fileName, col_types = readr::cols())
  expect_equal(nrow(afterNoOp), 2)
})

test_that("writeToCsv writes snake_case and supports incremental replacement", {
  fileName <- tempfile(fileext = ".csv")
  on.exit(unlink(fileName, force = TRUE), add = TRUE)

  CohortDiagnostics:::writeToCsv(
    data = data.frame(cohortId = 1, cohortEntries = 10),
    fileName = fileName,
    incremental = FALSE
  )

  first <- readr::read_csv(fileName, col_types = readr::cols())
  expect_true(all(c("cohort_id", "cohort_entries") %in% names(first)))

  CohortDiagnostics:::writeToCsv(
    data = data.frame(cohortId = 1, cohortEntries = 22),
    fileName = fileName,
    incremental = TRUE,
    cohortId = 1
  )

  second <- readr::read_csv(fileName, col_types = readr::cols())
  expect_equal(nrow(second), 1)
  expect_equal(second$cohort_entries[[1]], 22)
})

test_that("private utility helpers behave as expected", {
  folder <- tempfile("cd-folder-")
  expect_false(dir.exists(folder))

  result <- CohortDiagnostics:::createIfNotExist(type = "folder", name = folder)
  expect_true(dir.exists(folder))
  expect_s3_class(result, "AssertCollection")

  swapped <- CohortDiagnostics:::swapColumnContents(
    df = data.frame(targetId = c(1, 2), comparatorId = c(10, 20)),
    column1 = "targetId",
    column2 = "comparatorId"
  )
  expect_equal(swapped$targetId, c(10, 20))
  expect_equal(swapped$comparatorId, c(1, 2))

  df <- data.frame(a = c(1, 4), b = c(NA, 1))
  censored <- CohortDiagnostics:::enforceMinCellValueInDataframe(
    data = df,
    columnNames = c("a", "b", "missing"),
    minCellCount = 3
  )
  expect_equal(censored$a, c(-3, 4))
  expect_equal(censored$b, c(NA, -3))

  expect_equal(CohortDiagnostics:::naToZero(c(1, NA, 3)), c(1, 0, 3))
  expect_equal(CohortDiagnostics:::nullToEmpty(NULL), "")
  expect_equal(CohortDiagnostics:::titleCaseToCamelCase("Hello World"), "helloWorld")

  asInt <- CohortDiagnostics:::getTimeAsInteger(time = as.POSIXct("2024-01-01 00:00:00", tz = "UTC"))
  expect_true(is.numeric(asInt))
  expect_length(asInt, 1)
})

test_that("checkInputFileEncoding validates ASCII and rejects invalid encoding", {
  asciiFile <- tempfile(fileext = ".txt")
  on.exit(unlink(asciiFile, force = TRUE), add = TRUE)
  writeLines("abc", asciiFile, useBytes = TRUE)
  expect_true(CohortDiagnostics:::checkInputFileEncoding(asciiFile))

  badEncodingFile <- tempfile(fileext = ".txt")
  on.exit(unlink(badEncodingFile, force = TRUE), add = TRUE)
  writeBin(as.raw(c(0xff, 0xfe, 0x61, 0x00, 0x62, 0x00)), badEncodingFile)

  expect_error(CohortDiagnostics:::checkInputFileEncoding(badEncodingFile), "Illegal encoding")
})

test_that("timeExecution writes and appends execution records", {
  folder <- tempfile("cd-time-")
  dir.create(folder)
  on.exit(unlink(folder, recursive = TRUE, force = TRUE), add = TRUE)

  first <- CohortDiagnostics:::timeExecution(
    exportFolder = folder,
    taskName = "task_a",
    cohortIds = c(1, 2),
    expr = { Sys.sleep(0.001) }
  )
  expect_equal(nrow(first), 1)

  second <- CohortDiagnostics:::timeExecution(
    exportFolder = folder,
    taskName = "task_b",
    parent = "parent",
    cohortIds = NULL,
    start = "custom",
    execTime = "1s"
  )

  expect_equal(second$startTime[[1]], "custom")
  expect_equal(second$executionTime[[1]], "1s")

  out <- readr::read_csv(file.path(folder, "executionTimes.csv"), col_types = readr::cols())
  expect_equal(nrow(out), 2)
})

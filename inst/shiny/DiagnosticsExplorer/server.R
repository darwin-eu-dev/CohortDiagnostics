shiny::shinyServer(function(input, output, session) {
  connection <- DatabaseConnector::connect(shinySettings$connectionDetails)
  safeDisconnect <- function() {
    try(DatabaseConnector::disconnect(connection), silent = TRUE)
  }
  on.exit(safeDisconnect(), add = TRUE)
  session$onSessionEnded(function() {
    safeDisconnect()
  })

  availableTables <- sort(unique(c(
    CohortDiagnostics::getResultsDataModelSpecifications()$tableName,
    shinySettings$cohortTableName,
    shinySettings$databaseTableName
  )))

  formatSelection <- function(values, emptyLabel) {
    if (is.null(values) || length(values) == 0 || all(values == "")) {
      return(emptyLabel)
    }
    paste(values, collapse = ", ")
  }

  currentFilterContext <- shiny::reactive({
    databaseLabel <- formatSelection(input$database_id, "all databases")
    cohortLabel <- formatSelection(input$cohort_id, "all cohorts")
    rowLimit <- if (is.null(input$row_limit)) 200 else as.integer(input$row_limit)

    paste0(
      "Filters applied: databases = ", databaseLabel,
      "; cohorts = ", cohortLabel,
      "; row limit = ", rowLimit, "."
    )
  })

  loadReferenceData <- function() {
    databaseData <- queryTableSafe(
      connection = connection,
      schema = shinySettings$resultsDatabaseSchema,
      tableName = shinySettings$databaseTableName,
      limit = 10000
    )

    cohortData <- queryTableSafe(
      connection = connection,
      schema = shinySettings$resultsDatabaseSchema,
      tableName = shinySettings$cohortTableName,
      limit = 10000
    )

    databaseColumn <- normalizeColumnName(databaseData, c("database_id", "databaseId"))
    cohortIdColumn <- normalizeColumnName(cohortData, c("cohort_id", "cohortId"))
    cohortNameColumn <- normalizeColumnName(cohortData, c("cohort_name", "cohortName"))

    databaseChoices <- if (is.null(databaseColumn)) character(0) else sort(unique(databaseData[[databaseColumn]]))

    cohortChoices <- character(0)
    if (!is.null(cohortIdColumn)) {
      cohortIds <- cohortData[[cohortIdColumn]]
      cohortNames <- if (is.null(cohortNameColumn)) cohortIds else cohortData[[cohortNameColumn]]
      cohortChoices <- setNames(as.character(cohortIds), paste0(cohortIds, " - ", cohortNames))
    }

    list(databaseChoices = databaseChoices, cohortChoices = cohortChoices)
  }

  observeEvent(c(TRUE, input$refresh), {
    refs <- loadReferenceData()

    shiny::updateSelectInput(
      session = session,
      inputId = "database_id",
      choices = refs$databaseChoices,
      selected = refs$databaseChoices
    )

    shiny::updateSelectInput(
      session = session,
      inputId = "cohort_id",
      choices = refs$cohortChoices,
      selected = character(0)
    )

    shiny::updateSelectInput(
      session = session,
      inputId = "table_name",
      choices = availableTables,
      selected = shinySettings$cohortTableName
    )
  }, ignoreInit = FALSE)

  cohortCountsData <- shiny::reactive({
    rowLimit <- if (is.null(input$row_limit)) 200L else as.integer(input$row_limit)

    counts <- queryTableSafe(
      connection = connection,
      schema = shinySettings$resultsDatabaseSchema,
      tableName = paste0(shinySettings$tablePrefix, "cohort_count"),
      limit = rowLimit
    )

    filterDiagnosticsData(
      data = counts,
      databaseIds = input$database_id,
      cohortIds = input$cohort_id
    )
  })

  output$summary_filter_context <- shiny::renderText({
    currentFilterContext()
  })

  output$cohort_counts_filter_context <- shiny::renderText({
    currentFilterContext()
  })

  output$summary_table <- shiny::renderTable({
    cohortCounts <- cohortCountsData()

    if (nrow(cohortCounts) == 0) {
      return(data.frame(metric = character(0), value = numeric(0)))
    }

    entryColumn <- normalizeColumnName(cohortCounts, c("cohort_entries", "cohortEntries"))
    subjectColumn <- normalizeColumnName(cohortCounts, c("cohort_subjects", "cohortSubjects"))

    totalEntries <- if (is.null(entryColumn)) NA_real_ else sum(cohortCounts[[entryColumn]], na.rm = TRUE)
    totalSubjects <- if (is.null(subjectColumn)) NA_real_ else sum(cohortCounts[[subjectColumn]], na.rm = TRUE)

    data.frame(
      metric = c("Number of rows returned", "Total cohort entries", "Total cohort subjects"),
      value = c(nrow(cohortCounts), totalEntries, totalSubjects)
    )
  }, striped = TRUE, bordered = TRUE, spacing = "xs")

  output$summary_empty <- shiny::renderText({
    emptyMessage(cohortCountsData())
  })

  output$cohort_counts_table <- shiny::renderTable({
    cohortCountsData()
  }, striped = TRUE, bordered = TRUE, spacing = "xs")

  output$cohort_counts_empty <- shiny::renderText({
    emptyMessage(cohortCountsData())
  })

  tablePreviewData <- shiny::reactive({
    selectedTable <- input$table_name
    shiny::req(selectedTable)

    rowLimit <- if (is.null(input$row_limit)) 200L else as.integer(input$row_limit)

    data <- queryTableSafe(
      connection = connection,
      schema = shinySettings$resultsDatabaseSchema,
      tableName = selectedTable,
      limit = rowLimit
    )

    filterDiagnosticsData(
      data = data,
      databaseIds = input$database_id,
      cohortIds = input$cohort_id
    )
  })

  output$table_preview_context <- shiny::renderText({
    selectedTable <- if (is.null(input$table_name) || !nzchar(input$table_name)) "none" else input$table_name
    paste0("Selected table: ", selectedTable, ". ", currentFilterContext())
  })

  output$table_preview <- shiny::renderTable({
    tablePreviewData()
  }, striped = TRUE, bordered = TRUE, spacing = "xs")

  output$table_preview_empty <- shiny::renderText({
    emptyMessage(tablePreviewData())
  })
})

cohortDiagnosticsUi <- function() {
  shiny::fluidPage(
    shiny::tags$head(
      shiny::tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
    ),
    shiny::div(
      class = "app-shell",
      shiny::div(
        class = "app-header",
        shiny::img(src = "ohdsi-logo.svg", alt = "OHDSI logo", class = "app-logo"),
        shiny::div(
          class = "app-title-wrap",
          shiny::h1("Cohort Diagnostics Explorer", class = "app-title"),
          shiny::p(
            "Filter by database and cohort, then review summary totals, detailed cohort counts, or raw table rows.",
            class = "app-subtitle"
          )
        )
      ),
      shiny::sidebarLayout(
        shiny::sidebarPanel(
          class = "sidebar-panel",
          shiny::h3("Filters", class = "panel-title"),
          shiny::selectInput(
            inputId = "database_id",
            label = "Database(s)",
            choices = character(0),
            multiple = TRUE
          ),
          shiny::selectInput(
            inputId = "cohort_id",
            label = "Cohort ID(s)",
            choices = character(0),
            multiple = TRUE
          ),
          shiny::selectInput(
            inputId = "table_name",
            label = "Data table",
            choices = character(0),
            multiple = FALSE
          ),
          shiny::numericInput(
            inputId = "row_limit",
            label = "Maximum rows to display",
            value = 200,
            min = 1,
            max = 10000,
            step = 50
          ),
          shiny::actionButton("refresh", "Reload choices", class = "primary-action"),
          shiny::p(
            "Tip: Leaving filters blank shows all available rows for the selected table.",
            class = "sidebar-help"
          )
        ),
        shiny::mainPanel(
          class = "main-panel",
          shiny::tabsetPanel(
            id = "tabs",
            shiny::tabPanel(
              title = "Summary",
              shiny::div(
                class = "tab-copy",
                shiny::strong("What this shows:"),
                shiny::p(
                  "Aggregate totals from cohort_count for the current filters: number of rows, total cohort entries, and total cohort subjects."
                ),
                shiny::div(class = "context-line", shiny::textOutput("summary_filter_context"))
              ),
              shiny::tableOutput("summary_table"),
              shiny::div(class = "empty-line", shiny::textOutput("summary_empty"))
            ),
            shiny::tabPanel(
              title = "Cohort Counts",
              shiny::div(
                class = "tab-copy",
                shiny::strong("What this shows:"),
                shiny::p(
                  "Row-level cohort_count records after applying the selected database and cohort filters."
                ),
                shiny::div(class = "context-line", shiny::textOutput("cohort_counts_filter_context"))
              ),
              shiny::tableOutput("cohort_counts_table"),
              shiny::div(class = "empty-line", shiny::textOutput("cohort_counts_empty"))
            ),
            shiny::tabPanel(
              title = "Table Browser",
              shiny::div(
                class = "tab-copy",
                shiny::strong("What this shows:"),
                shiny::p(
                  "A preview of the selected results table, filtered to the chosen databases/cohorts when those columns exist in that table."
                ),
                shiny::div(class = "context-line", shiny::textOutput("table_preview_context"))
              ),
              shiny::tableOutput("table_preview"),
              shiny::div(class = "empty-line", shiny::textOutput("table_preview_empty"))
            )
          )
        )
      )
    )
  )
}

shiny::shinyUI(cohortDiagnosticsUi())

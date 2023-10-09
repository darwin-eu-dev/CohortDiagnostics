## app.R ##
library(shinydashboard)
source("global.R")

cohort_counts_df <- readr::read_csv(here::here("data", "cohort_counts.csv"), col_types = "icii")
cohort_incidence_df <- readr::read_csv(here::here("data", "incidence.csv"), col_types = incidence_col_types)
time_distributions_df <- readr::read_csv(here::here("data", "time_distributions.csv"), col_types = "icid")
cohort_attrition_df <- readr::read_csv(here::here("data", "cohort_attrition.csv"), col_types = "iiiicii")
cohort_characterization_df <- readr::read_csv(here::here("data", "large_scale_characterization.csv"), col_types = characterization_col_types) %>% 
  tidyr::pivot_wider(names_from = "estimate_type", values_from = "estimate") %>% 
  dplyr::select(-c("result_type", "cdm_name"))

cohort_choices <- setNames(cohort_counts_df$cohort_definition_id,
                           cohort_counts_df$cohort_name)

ui <- dashboardPage(
  
  dashboardHeader(title = "Cohort Diagnostics"),
  
  dashboardSidebar(
    sidebarMenu(
      menuItem("Cohort Counts", tabName = "counts"),
      menuItem("Incidence Rates", tabName = "incidence"),
      menuItem("Time Distributions", tabName = "time_distributions"),
      menuItem("Cohort Attrition", tabName = "attrition"),
      menuItem("Cohort Characterization", tabName = "characterization")
    )
  ),
  
  dashboardBody(
    tabItems(
      tabItem(tabName = "counts",
              DT::DTOutput("cohort_counts")
              # fluidRow(
              #   box(plotOutput("plot1", height = 250)),
              #   box(title = "Controls", sliderInput("slider", "Number of observations:", 1, 100, 50))
              # )
      ),
      
      tabItem(tabName = "incidence", 
              selectInput("selected_cohort1", "Cohort", choices = cohort_choices),
              echarts4r::echarts4rOutput("incidence_plot1"),
              echarts4r::echarts4rOutput("incidence_plot2"),
              echarts4r::echarts4rOutput("incidence_plot3")
      ),
      
      tabItem(tabName = "time_distributions", 
              selectInput("selected_cohort2", "Cohort", choices = cohort_choices),
              echarts4r::echarts4rOutput("time_distribution_plot1"),
              echarts4r::echarts4rOutput("time_distribution_plot2"),
              echarts4r::echarts4rOutput("time_distribution_plot3")
      ),
      
      tabItem(tabName = "attrition", 
              selectInput("selected_cohort3", "Cohort", choices = cohort_choices),
              gt::gt_output("attrition_table")
      ),
      
      tabItem(tabName = "characterization", 
              selectInput("selected_cohort4", "Cohort", choices = names(cohort_choices)), # only have cohort name in PP output
              DT::DTOutput("characterization_table")
      )
    )
  )
)

server <- function(input, output) {
  
  output$cohort_counts <- DT::renderDataTable({
    DT::datatable(cohort_counts_df, rownames = F)
  })
  
  incidence_plots <- reactive({
    cohort_incidence_df %>% 
      dplyr::filter(.data$outcome_cohort_id == input$selected_cohort1) %>% 
      dplyr::select("incidence_start_date", "incidence_100000_pys", "denominator_age_group", "denominator_sex") %>% 
      {split(., .$denominator_age_group)} %>%
      purrr::map(function(df) {
        df %>% 
          dplyr::group_by(.data$denominator_sex) %>% 
          echarts4r::e_charts(incidence_start_date) %>%
          echarts4r::e_line(incidence_100000_pys, smooth = T) %>% 
          echarts4r::e_title(paste(unique(df$denominator_age_group))) %>% 
          echarts4r::e_tooltip(trigger = c("item", "axis"), axisPointer = list(type = "cross"))
      }) 
  })
  
  # Not sure if there is nicer way to do this. using e_arrange did not work.
  # see https://stackoverflow.com/questions/73891585/echarts4r-ee-arrange-not-displaying-in-shiny
  output$incidence_plot1 <- echarts4r::renderEcharts4r(incidence_plots()[[1]])
  output$incidence_plot2 <- echarts4r::renderEcharts4r(incidence_plots()[[2]])
  output$incidence_plot3 <- echarts4r::renderEcharts4r(incidence_plots()[[3]])
  
  time_distributions_plots <- reactive({
    x <- time_distributions_df %>% 
      dplyr::mutate(percentile = 100 - percentile) %>% 
      dplyr::filter(.data$cohort_definition_id == input$selected_cohort2) %>% 
      {split(., .$group)}
    
    purrr::map(x, function(df) {
      df %>% 
        echarts4r::e_charts(value) %>%
        echarts4r::e_line(percentile, smooth = T) %>% 
        echarts4r::e_title(stringr::str_to_title(stringr::str_replace_all(unique(df$group), "_", " "))) %>% 
        echarts4r::e_axis_labels(x = "Days", y = "Percentile") %>% 
        echarts4r::e_legend(show = F) %>% 
        echarts4r::e_tooltip(trigger = c("item", "axis"), axisPointer = list(type = "cross"))
    }) %>% 
      setNames(names(x))
  })
  
  output$time_distribution_plot1 <- echarts4r::renderEcharts4r(time_distributions_plots()[["prior_observation"]])
  output$time_distribution_plot2 <- echarts4r::renderEcharts4r(time_distributions_plots()[["time_in_cohort"]])
  output$time_distribution_plot3 <- echarts4r::renderEcharts4r(time_distributions_plots()[["future_observation"]])
  
  output$attrition_table <- gt::render_gt({
    cohort_attrition_df %>% 
      dplyr::filter(cohort_definition_id == input$selected_cohort3) %>% 
      dplyr::arrange(reason_id) %>% 
      dplyr::select("reason_id", 
                    "reason", 
                    "number_records", 
                    "number_subjects", 
                    "excluded_records", 
                    "excluded_subjects") %>% 
      gt::gt()
  })
  
  output$characterization_table <- DT::renderDataTable({
    cohort_characterization_df %>%
      dplyr::mutate(percentage = round(percentage, 2)) %>% 
      dplyr::filter(group_name == "Cohort name", group_level == input$selected_cohort4) %>%
      dplyr::arrange(dplyr::desc(percentage)) %>%
      dplyr::select(-c("group_name", "group_level", "strata_name", "strata_level")) %>%
      DT::datatable(selection = "none", rownames = F)
  })
  
}

shinyApp(ui, server)





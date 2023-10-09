



#' Execute Cohort Diagnostics
#'
#' @param cdm A cdm_reference object created with `CDMConnector::cdm_from_con()`
#' @param cohort_table_name The name of the cohort table in the CDM
#'
#' @return NULL. This function 
#' @export
#'
#' @examples
#' \dontrun{
#'   con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir("synthea-covid19-10k"))
#' cdm <- cdm_from_con(con, "main", "main")
#' 
#' cnts <- cdm$condition_occurrence %>% 
#'   count(condition_concept_id, sort = T) %>% 
#'   head() %>% 
#'   left_join(select(cdm$concept, "concept_id", "concept_name"), by = c("condition_concept_id" = "concept_id")) %>% 
#'   collect()
#' 
#' concept_sets <- as.list(cnts$condition_concept_id)
#' names(concept_sets) <- cnts$concept_name
#' 
#' cdm <- generate_concept_cohort_set(cdm, concept_sets)
#' 
#' cdm$cohort
#' 
#' }
#' 
#' 
executeDiagnostics <- function(cdm, cohort_table_name = "cohort", outputDir = here::here()) {
  

  
  # ----- 
  cohortSet(cdm[["cohort"]]) %>% 
    inner_join(cohortCount(cdm[["cohort"]]), by  = "cohort_definition_id") %>% 
    readr::write_csv(here::here("data", "cohort_counts.csv"))
  
  
  cohort_attrition(cdm[["cohort"]]) %>% 
    readr::write_csv(file.path("data", "cohort_attrition.csv"))
  
  
  # time distributions
  
  cd_time_distributions <- function(cdm, cohort_table_name = "cohort") {
    
    query <- cdm$cohort %>% 
      PatientProfiles::addDemographics() %>% 
      dplyr::mutate(time_in_cohort = !!datediff("cohort_start_date", "cohort_end_date"))
    
    df <- query %>% 
      dplyr::transmute(cohort_definition_id, group = "prior_observation", time = prior_observation) %>% 
      dplyr::union_all(dplyr::transmute(query, .data$cohort_definition_id, group = "time_in_cohort", time = .data$time_in_cohort)) %>% 
      dplyr::union_all(dplyr::transmute(query, .data$cohort_definition_id, group = "future_observation", time = .data$future_observation)) %>% 
      dplyr::group_by(cohort_definition_id, group) %>% 
      CDMConnector::summarise_quantile(time, probs = seq(0, 1, by = .05), name_suffix = "") %>% 
      dplyr::collect()
    
    tidyr::pivot_longer(df, cols = starts_with("p")) %>% 
      dplyr::mutate(percentile = as.integer(stringr::str_extract(name, "\\d+"))) %>% 
      dplyr::select(cohort_definition_id, group, percentile, value) %>% 
      readr::write_csv(file.path("data", "time_distributions.csv"))
  }
  
  attributes(cdm$cohort)
  
  
  large_scale_char <- PatientProfiles::summariseLargeScaleCharacteristics(
    cohort = cdm[["cohort"]],
    window = list(c(-Inf, -366), c(-365, -31), c(-30, -1), 
                  c(0, 0), 
                  c(1, 30), c(31, 365),  c(366, Inf)),
    
    eventInWindow = c("condition_occurrence", "drug_exposure", "visit_occurrence",
                      "measurement", "procedure_occurrence",  "observation"),
  )
  
  readr::write_csv(large_scale_char, file.path("data", "large_scale_characterization.csv"))
  
  
}
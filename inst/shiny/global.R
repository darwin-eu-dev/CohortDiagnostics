library(readr)

# This is the expected input from the IncidencePrevalence output
incidence_col_types <- cols(
  analysis_id = col_double(),
  n_persons = col_double(),
  person_days = col_double(),
  n_events = col_double(),
  incidence_start_date = col_date(format = ""),
  incidence_end_date = col_date(format = ""),
  person_years = col_double(),
  incidence_100000_pys = col_double(),
  incidence_100000_pys_95CI_lower = col_double(),
  incidence_100000_pys_95CI_upper = col_double(),
  cohort_obscured = col_logical(),
  result_obscured = col_logical(),
  outcome_cohort_id = col_double(),
  outcome_cohort_name = col_character(),
  analysis_repeated_events = col_logical(),
  analysis_interval = col_character(),
  analysis_complete_database_intervals = col_logical(),
  denominator_cohort_id = col_double(),
  analysis_outcome_washout = col_logical(),
  analysis_min_cell_count = col_double(),
  denominator_cohort_name = col_character(),
  denominator_age_group = col_character(),
  denominator_sex = col_character(),
  denominator_days_prior_history = col_double(),
  denominator_start_date = col_date(format = ""),
  denominator_end_date = col_date(format = ""),
  denominator_strata_cohort_definition_id = col_logical(),
  denominator_strata_cohort_name = col_logical(),
  denominator_closed_cohort = col_logical(),
  cdm_name = col_character()
)

# This is the expected input from the PatientProfiles::summariseLargeScaleCharacteristics output

characterization_col_types <- cols(
  result_type = col_character(),
  cdm_name = col_character(),
  group_name = col_character(),
  group_level = col_character(),
  strata_name = col_character(),
  strata_level = col_character(),
  table_name = col_character(),
  type = col_character(),
  analysis = col_character(),
  concept = col_double(),
  variable = col_character(),
  variable_level = col_character(),
  estimate_type = col_character(),
  estimate = col_double()
)



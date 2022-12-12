# Source project utilities
source('/home/scripts/utilities.R')
library(dtplyr)
# Source raw data from snowflake tables that are populated by Curriculum Portal Workflow
data <- snowflake_query("SELECT a.learningresourcename as learning_resource_name,
 a.id as learning_resource_id,
 a.SKILLPATHPOSITION as skill_path_position,
 a.stage,
 a.status,
 a.UPDATEDAT as updated_at,
 a.RECOMMENDEDCONTENTTYPES AS content_type,
 a.validated,
 i.guid as opportunity_id,
 i.CONTENTID as content_id,
 a.createdat as created_date,
 e.instructionalPlanId as plan_id,
 a.lengthinminutes as learning_resource_length_min,
 lvl1.preflabel as level_1,
 lvl2.preflabel as level_2
 FROM dvs.current_state.skills_curriculumWorkflow_v1_ContentPlan a
LEFT JOIN DVS.CURRENT_STATE.SKILLS_TAGASSIGNMENTS_V2_ASSIGNMENTS b ON a.id = b.contentid
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl1 on b.level1 = lvl1.id
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl2 on b.level2 = lvl2.id
LEFT JOIN dvs.CURRENT_STATE.SKILLS_CONTENTAUTHORINGHOME_V4_OPPORTUNITY i ON a.id = i.planid
left join dvs.CURRENT_STATE.skills_curriculumWorkflow_v1_InstructionalPlanContentPlan e ON a.id = e.contentPlanId
WHERE a.validated = TRUE
ORDER BY updated_at DESC") %>%
  # filter to only include video and cloud course content
  filter(str_detect(tolower(content_type), 'video|cloud course'))

# get existing offers already written to snowflake
current_offers <- snowflake_query("select * from analytics.uncertified.PRODUCTION_OFFERS_TABLE ORDER BY created_date DESC")
# get existing predictions
current_predicts <- snowflake_query("select * from analytics.uncertified.PRODUCTION_PREDS_TABLE")

good_offers <- current_offers %>%
  filter(!is.na(preliminary_royalty_rate) & !is.na(preliminary_second_cp))

# Filter again to remove duplicates and gather only necessary columns for input into automated offers script
offer_data <- data %>%
  filter(!learning_resource_id %in% good_offers$learning_resource_id,
         !is.na(learning_resource_length_min),
         !learning_resource_name %in% good_offers$learning_resource_name,
  learning_resource_length_min > 0,
         !is.na(level_1)) %>%
  select(learning_resource_id
                , learning_resource_name
                , learning_resource_length_min
                , level_1
                , level_2
                , skill_path_position) %>%
  distinct() %>%
  mutate(skill_path_position = as.numeric(ifelse(is.na(skill_path_position), "0", skill_path_position)),
         level_1 = tolower(level_1),
         level_2 = tolower(level_2)) %>% 
  rename('course_title' = 'learning_resource_name',
                "path_position" = 'skill_path_position',
         'duration_in_hours' = 'learning_resource_length_min')


#### get offers ####
if(nrow(offer_data) > 0) {
  # Set functino for royalty offers
  source("/home/scripts/automated_offers.R")
  # Apply function for new incoming courses
  msg <- royalty_offers(offer_data)
} else {
  temp_predictions_df <- data.frame()
  temp_offers_df <- data.frame()
  offers_df <- data.frame()
  final_predictions <- data.frame()
  msg <- TRUE
  o_result <- c()
  p_result <- c()
}

# If logic to remove duplicated courses that already have offers and write new ones into snowflake table to be populated into tableau
if(nrow(temp_predictions_df)>0){
final_predictions <- current_predicts %>% mutate(created_date = as_datetime(created_date)) %>%
  filter(!offer_id %in% temp_predictions_df$offer_id) %>%
  bind_rows(temp_predictions_df)
snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.PRODUCTION_PREDS_TABLE")
# Write to snowflake
letItSnow(final_predictions, overwrite = TRUE, tableName = "PRODUCTION_PREDS_TABLE", schema = "UNCERTIFIED")
}
# If logic to remove duplicate offer courses and write new ones into snowflake table
if(nrow(temp_offers_df)>0){
offers_df <- temp_offers_df %>%
  filter(!is.na(preliminary_second_cp)) %>%
  bind_rows(good_offers)
snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.PRODUCTION_OFFERS_TABLE")
# Write to snowflake
letItSnow(offers_df, overwrite = TRUE, tableName = "PRODUCTION_OFFERS_TABLE", schema = "UNCERTIFIED")
}
# Messaging logic to inform if workflow completed successfully
if(nrow(offers_df) > 0 &
                nrow(final_predictions) > 0 &
                msg == "TRUE" &
                !any(is(offers_df, "try-error")) &
                !any(is(final_predictions, "try-error"))){
  pushover.r("SUCCESS - automated pricing")
  final_scheduled_msg <- paste0('Scheduled script completed!\n\n', nrow(temp_offers_df), ' offer(s) created from pricing automation')
  pushover.r(final_scheduled_msg)
} else if (nrow(offer_data) < 1 & 
           nrow(temp_predictions_df) == 0 &
           nrow(temp_offers_df) == 0) {
  pushover.r("Pricing Automation - SUCCESS: No new courses to generate offers for!")
  } else {
  pushover.r("FAILED - automated pricing")
}

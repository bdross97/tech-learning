#source("~/SageMaker/efs/global.R")
library(dplyr)
library(odbc)
source('~/SageMaker/efs/author_compensation/utils')

opportunity_df <- snowflake_query("select
  a.title,
  a.guid as opp_id,
  a.planid as learning_resource_id,
  a.contentid as content_id,
  a.updateddate as opp_updated,
  a.createddate as opp_created,
  a.status,
  a.contenttype as opp_type,
  b.handle as asm,
  b.username
  FROM dvs.current_state.exp_contentauthoringhome_v1_opportunity a 
  left join dvs.current_state.exp_identity_user b ON b.handle = SUBSTR(TO_CHAR(a.teammembers), 3, 36)
  WHERE b.username NOT LIKE 'SLChris'", checkCache = FALSE)


opportunity_df <- opportunity_df %>%
  mutate(opp_date = pmax(opp_updated, opp_created, na.rm = TRUE),
         opp_updated = ifelse(opp_updated == "", NA, opp_updated)) %>%
  group_by(opp_id) %>%
  filter(opp_date == max(opp_date),
         status != "removed") %>%
  ungroup()

asm_list <- opportunity_df %>%
  select(asm, username) %>%
  distinct()

asms <- snowflake_query("select distinct b.username, b.handle as asm from dvs.current_state.exp_contentauthoringhome_v1_opportunity a 
  left join dvs.current_state.exp_identity_user b ON b.handle = SUBSTR(TO_CHAR(a.teammembers), 3, 36)
  WHERE b.username NOT LIKE 'SLChris'")

letItSnow(asm_list, tableName = 'asm_list', overwrite = TRUE)

letItSnow(opportunity_df, tableName = 'test_opp_status', overwrite = TRUE)

#Library Loading
library(stringr)
library(dplyr)
library(tidyr)
#Source utilities file
source("~/SageMaker/efs/content_strategy/brayden_ross/utilities.R")

#Snowflake query to load learning plans
stage_content_plans <- snowflake_query("select
 a.learningresourcename,
 a.id,
 a.SKILLPATHPOSITION,
 a.status,
 a.UPDATEDAT,
 a.RECOMMENDEDCONTENTTYPES AS content_type
 ,a.createdat
 ,a.lengthinminutes AS duration_in_minutes
 ,c.name as TAGNAME
 ,d.name as FACET
 ,e.instructionalPlanId as plan_id
 ,f.skillId as skill_id
 ,g.roleId as role_id
 ,h.name as role_name
 ,i.GUID as opportunity_id
from dvs.current_state.exp_curriculum_v1_contentplan a
left join dvs.current_state.exp_tag_management_v4_assignment b on a.id = b.contentid
left join dvs.current_state.exp_tag_management_v5_tag c on b.tagid = c.id
left join dvs.current_state.exp_tag_management_v4_facet d on b.facetid = d.id
left join dvs.CURRENT_STATE.EXP_CURRICULUM_V1_INSTRUCTIONALPLANCONTENTPLAN e ON a.id = e.contentPlanId
left join dvs.CURRENT_STATE.EXP_CURRICULUM_V1_SKILLINSTRUCTIONALPLAN f ON e.instructionalPlanId = f.instructionalPlanId
left join dvs.CURRENT_STATE.EXP_CURRICULUM_V4_ROLESKILL g ON f.skillId = g.skillId
left join dvs.CURRENT_STATE.EXP_CURRICULUM_V4_ROLE h ON g.roleId = h.id
left join dvs.CURRENT_STATE.EXP_CONTENTAUTHORINGHOME_V3_OPPORTUNITY i ON a.id = i.planid
where duration_in_minutes > 0 AND STAGE LIKE 'New Opportunity'
order by a.createdat, a.learningresourcename DESC") %>%
  filter(str_detect(content_type, 'Video Course')) %>%
  spread(facet, tagname) %>%
  select(-"<NA>")



#Naming table and writing to snowflake, keeping name convention to ensure seamlessness between old and new files
letItSnow(data = stage_content_plans, tableName = "learning_plans_from_api", overwrite = TRUE)

###############################################
###############################################
######  THIS VERSION OF THE FILE IS MADE ######
###### SPECIFICALLY FOR IPS FROM THE API ######
###############################################
###############################################

royalty_offers <- function(offer_data){
  #### REMOVE THIS WHEN TOP 100 ARE FIXED ####
  library(stringr)
  top_100_slugs <- c('react', 'angular', 'csharp', 'react js', 'python', 'comptia', 'dotnet',
                     'aspnet', 'sql', 'kubernetes', 'docker', 'cplusplus', 'linq', 'c sharp', 'hacking', 'javascript',
                     'java', 'aws', 'powershell', 'apache', 'kafka', 'agile', 'devops', 'power bi', 'data analytics', 'aspdotnet',
                     'html', 'jenkins', 'maven', 'azure', ' ng ', 'lfcs', 'terraform', 'patterns', 'azure', 'mvc4', 'cloud computing', 'nodejs',
                     'html5', 'angularjs', 'bash', 'jquery', 'go', 'cqrs', 'asp.net', 'c#','dot net', 'css', 'spring', 'typescript', 'git')
  options(dplyr.summarise.inform = FALSE)
  # Model Loading
  source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'/utils/utilities.R'))
  library(dtplyr)
  cp_model_core <- readRDS(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"/video/video_comp_model/Model Storage/ols_model.RDS"))
  royalty_revenue <- snowflake_query("SELECT 
                                     date_month as usage_year_month,
                                     seconds AS royalty_revenue
                                     FROM UNCERTIFIED.ABCD")
  platform_size <- snowflake_query("Select *
From UNCERTIFIED.PX_VIDEO_PLATFORM_SIZE")
  
  if(
    platform_size %>%
    filter(usage_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    nrow() < 36
  ){
    for(x in seq(1:3)){
      try(pushoverr::pushover(message = "PLAT SIZE RAN OUT!",
                              user = Sys.getenv("PUSH_USR"), app = Sys.getenv("PUSH_PWD")))
      Sys.sleep(5)
    }
    return("PLAT")
  }
  
  royalty_revenue <- royalty_revenue %>%
    filter(usage_year_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    arrange(usage_year_month) %>%
    mutate(course_age = row_number()) %>%
    filter(course_age <= 36)
  
  if(nrow(royalty_revenue)<36){
    for(x in seq(1:3)){
      try(pushover.r("REV PROJECTIONS RAN OUT!"))
      Sys.sleep(5)
    }
    return("REV")
  }
  
  size_rev <- platform_size %>%
    filter(usage_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    right_join(royalty_revenue, by = c("usage_month"="usage_year_month"))
  
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
    filter(str_detect(tolower(content_type), 'video')) %>%
    spread(facet, tagname) %>%
    select(-"<NA>") %>%
    mutate(month = lubridate::floor_date(date(updatedat), "month"))
  
  
  
  atomic_count <- stage_content_plans %>%
    group_by(`Primary Atomic`, month) %>%
    summarise(atomic_count = n()) %>%
    rename(primary_atomic_tag = `Primary Atomic`) %>%
    filter(!is.na(primary_atomic_tag)) %>%
    mutate(primary_atomic_tag = tolower(primary_atomic_tag))
  
  domain_count <- stage_content_plans %>%
    group_by(`Primary Domain`, month) %>%
    summarise(domain_count = n()) %>%
    rename(primary_domain_tag = `Primary Domain`) %>%
    filter(!is.na(primary_domain_tag)) %>%
    mutate(primary_domain_tag = tolower(primary_domain_tag))
  
  
  # Exponential decay
  exp_decay <- function(target){
    rate = (target^(1/36)) - 1
    temp_df <- data.frame(month = 1:36)
    temp_df <- temp_df %>%
      mutate(total_royalties = 1*(1+rate)^month)
    
    difference = diff(temp_df$total_royalties)
    difference <- c(0,difference)
    
    temp_df$MoM_royalties <- round(difference,2)
    
    final_projections <- temp_df %>%
      arrange(desc(MoM_royalties)) %>%
      mutate(month = 1:36) %>%
      select(-total_royalties)
    
    return(final_projections)
  }
  
  # Factor levels in cp_model_core
  domain_levels <- cp_model_core$xlevels$primary_domain_tag
  atomic_levels <- cp_model_core$xlevels$primary_atomic_tag
  
  # New Comp Targets from SF
  comp_targets <- snowflake_query('select * from analytics.uncertified.vw_compensation_targets')
  
  #### Cycle Pending Courses ####
  temp_offers_df <<- data.frame()
  temp_predictions_df <<- data.frame()
  errors_df <<- data.frame()
  # Progress bar
  # pb = txtProgressBar(min = 0, max = nrow(offer_data), initial = 0) 
  
  for(i in seq(1:nrow(offer_data))){
    course <- offer_data[i,]
    res <- grep(x = tolower(course$course_title), pattern = paste(top_100_slugs,collapse="|"))
    if(length(res) > 0) {
      top_100_variable <- TRUE
    } else {
      top_100_variable <- FALSE
    }
    #Get individual course
    
    #Rename Primary Domain Tag
    course <- course %>%
      rename(primary_domain_tag = `Primary Domain`,
                    primary_super_domain = `Primary Super Domain`,
                    primary_atomic_tag = `Primary Atomic`,
                    learning_resource_name = course_title,
                    primary_sub_domain = `Primary Sub Domain`)
    
    #Course style patches
    course <- course %>%
      mutate_if(is.character, tolower) %>%
      mutate(course_style_tag = if_else(course_style_tag == "building"
                                               , "building x with y"
                                               , course_style_tag)
                    , course_style_tag = if_else(course_style_tag == "beginner"
                                                 , "best practices"
                                                 , course_style_tag)
                    , course_style_tag = if_else(course_style_tag == "exam briefing"
                                                 , "advanced"
                                                 , course_style_tag)
                    , course_style_tag = if_else(course_style_tag == "practical application"
                                                 , "best practices"
                                                 , course_style_tag)
                    , course_style_tag = if_else(course_style_tag == "other"
                                                 , "best practices"
                                                 , course_style_tag)
                    , course_style_tag = if_else(course_style_tag == "what's new",
                                                 cp_model_core$xlevels$course_style_tag[which(cp_model_core$xlevels$course_style_tag == "what's new")],
                                                 course_style_tag)
                    , course_style_tag = if_else(!course_style_tag %in% cp_model_core$xlevels$course_style_tag,
                                                 "best practices", course_style_tag)
                    # THIS NEXT LINE NEEDS TO BE DELETED WHEN THE MODEL IS FIXED
                    , primary_super_domain = if_else(primary_super_domain == "languages, frameworks and tools", "languages, frameworks, and tools", primary_super_domain)
      )
    
    
    
    is_cert <- 0
    is_cert <- as.logical(is_cert)
    
    is_course_retired <- 0
    is_course_retired <- as.logical(is_course_retired)
    
    course_was_refreshed <- 0
    course_was_refreshed <- as.logical(course_was_refreshed)
    
    domain_count_loop <- domain_count %>%
      filter(primary_domain_tag == course$primary_domain_tag)
    domain_count_loop <- domain_count_loop %>%
      filter(month == max(domain_count_loop$month)) %>% #select most recent number
      select(primary_domain_tag, domain_count)
    primary_domain_saturation <- as.numeric(domain_count_loop[1,2]) 
    primary_domain_saturation <- ifelse(is.na(primary_domain_saturation), as.numeric(0), as.numeric(primary_domain_saturation))
    
    atomic_count_loop <- atomic_count %>%
      filter(primary_atomic_tag == course$primary_atomic_tag)
    atomic_count_loop <- atomic_count_loop %>%
      filter(month == max(atomic_count_loop$month)) %>% #select most recent number
      select(primary_atomic_tag, atomic_count)
    primary_atomic_saturation <- as.numeric(atomic_count_loop[1,2])
    primary_atomic_saturation <- ifelse(is.na(primary_atomic_saturation), as.numeric(0), as.numeric(primary_atomic_saturation))
    
    super_domain_levels <- cp_model_core$xlevels$primary_super_domain
    domain_levels <- cp_model_core$xlevels$primary_domain_tag
    subDomain_levels <- cp_model_core$xlevels$primary_sub_domain
    atomic_levels <- cp_model_core$xlevels$primary_atomic_tag
    
    size_rev <- size_rev %>%
      arrange(course_age)
    
    model_input <- course %>%
      cbind(size_rev) %>%
      left_join(domain_count_loop, by = 'primary_domain_tag') %>%
      left_join(atomic_count_loop, by = "primary_atomic_tag") %>%
      mutate(domain_count = as.numeric(ifelse(is.na(domain_count), 0, domain_count)),
             atomic_count = as.numeric(ifelse(is.na(atomic_count), 0, atomic_count))) %>%
      mutate(duration_in_hours = as.numeric(learning_resource_length_min)/60
                    , is_cert = if_else(learning_resource_name %like% "%certif%", TRUE, FALSE)
                    , is_course_retired = FALSE
                    , course_was_refreshed = FALSE
                    , course_age2 = course_age^2
                    , average_position = if_else(as.numeric(course$path_position) > 5 | as.numeric(course$path_position) == 0 | is.na(course$path_position),
                                                 0, switch(EXPR = as.numeric(course$path_position), 5, 4, 3, 2, 1))) %>%
      mutate(primary_domain_tag = as.character(primary_domain_tag)
                    , primary_domain_tag = if_else(primary_domain_tag %in% domain_levels,
                                                   primary_domain_tag, "Other")
                    , primary_atomic_tag = as.character(primary_atomic_tag)
                    , primary_atomic_tag = if_else(primary_atomic_tag %in% atomic_levels,
                                                   primary_atomic_tag, "Other")
                    , primary_sub_domain = as.character(primary_sub_domain)
                    , primary_sub_domain = if_else(primary_sub_domain %in% subDomain_levels,
                                                   primary_sub_domain, "Other")
                    , primary_super_domain = as.character(primary_super_domain)
                    , primary_super_domain = if_else(primary_super_domain %in% super_domain_levels,
                                                     primary_super_domain, "data")) %>%
      mutate(primary_super_domain = as.factor(primary_super_domain)
                    , primary_domain_tag = as.factor(primary_domain_tag)
                    , primary_atomic_tag = as.factor(primary_atomic_tag)
                    , primary_sub_domain = as.factor(primary_sub_domain)
                    , primary_domain_saturation = primary_domain_saturation
                    , primary_atomic_saturation = primary_atomic_saturation)
    
    model_input <- model_input %>%
      select(-course_age2)
    
    # Add a unique identifier for each offer
    row_offer_id <- uuid::UUIDgenerate(use.time = TRUE)
    
    # Important objects used in determining course offers
    comp_payment_1 <- max((model_input$duration_in_hours+1)*1000, 1500)
    
    duration_in_hours <- mean(model_input$duration_in_hours)
    duration_in_hours <- ceiling(duration_in_hours * 4) / 4
    #update
    target <- comp_targets[comp_targets$duration_in_hours == duration_in_hours,][[2]]
    
    #### Model Predictions ####
    predicts_df <- try(data.frame(predictions = predict(object = cp_model_core
                                                            , newdata = model_input)
                                  , month = seq(1:36)
    ))
    
    
    if(!is(predicts_df, 'try-error')){
      
      predicts_df <- predicts_df %>%
        mutate(predictions = if_else(predictions < 0.00001224153,
                                     0.00001224153,
                                     if_else(predictions > 0.002046639,
                                             0.002046639, predictions))) %>%
        left_join(royalty_revenue, by = c("month" = "course_age")) %>%
        mutate(attributable_revenue = predictions * royalty_revenue)
      
      
      if(top_100_variable == TRUE) {
        royalty_rate_1 <- target/sum(predicts_df$attributable_revenue)
        predicts_df <- predicts_df %>%
          mutate(royalty_rate = royalty_rate_1
                 , royalty_rate = ifelse(royalty_rate > .10, .10, royalty_rate)
                 , royalty_rate = ifelse(royalty_rate < .06, .06, royalty_rate)
                 , royalty_pmt = round(royalty_rate * attributable_revenue, 2)
          )
        
      } else {
        royalty_rate_1 <- target/sum(predicts_df$attributable_revenue)
        predicts_df <- predicts_df %>%
          mutate(royalty_rate = royalty_rate_1
                 , royalty_rate = ifelse(royalty_rate > .15, .15, royalty_rate)
                 , royalty_rate = ifelse(royalty_rate < .06, .06, royalty_rate)
                 , royalty_pmt = round(royalty_rate * attributable_revenue, 2)
          )
      }
      total_royalties <- sum(predicts_df$royalty_pmt)
      total_comp = total_royalties + comp_payment_1
      
      if(total_royalties > target*3
         | total_comp < target
      ){
        temp_df <- exp_decay(target)
        predicts_df$royalty_pmt <- temp_df$MoM_royalties
      }else{
        predicts_df <- predicts_df
      } 
      
      
      # Determine royalty rate for Offer 2
      predicts_df <- predicts_df %>%
        mutate(second_rr = .05
               , second_royalty_pmt = (second_rr/royalty_rate)*royalty_pmt
               , second_cp = round(min((sum(royalty_pmt)-sum(second_royalty_pmt))/2+comp_payment_1
                                       , 10000), -2)
               , created_at = now())
      
      
      
      # Apply additional decision rules for Executive Briefings and Minimum Completion Payments
      comp_payment_1 = ifelse(course$course_style_tag == "executive briefing",
                              comp_payment_1 + 2000,
                              comp_payment_1)
      
      comp_payment_2 = round(mean(predicts_df$second_cp), 2)
      comp_payment_2 = ifelse(comp_payment_2 < 4500,
                              4500,
                              comp_payment_2)
      
      comp_payment_2 = ifelse(course$course_style_tag == "executive briefing",
                              comp_payment_2 + 2000,
                              comp_payment_2)
      
      #Consolidate course offers
      temp_df <- data.frame(preliminary_royalty_rate = round(mean(predicts_df$royalty_rate),3),
                            preliminary_completion_payment = comp_payment_1,
                            preliminary_royalties = sum(predicts_df$royalty_pmt),
                            preliminary_second_rr = mean(predicts_df$second_rr),
                            preliminary_second_cp = comp_payment_2,
                            preliminary_second_royalties = sum(predicts_df$second_royalty_pmt),
                            learning_resource_id = course$learning_resource_id,
                            learning_resource_name = course$learning_resource_name,
                            created_date = min(predicts_df$created_at),
                            offer_id = row_offer_id,
                            stringsAsFactors = FALSE)
      
      
      
      temp_offers_df <<- bind_rows(temp_offers_df, temp_df)
      
      
      
      temp_p_df <- predicts_df %>%
        mutate(predictions = as.numeric(predictions)) %>%
        rename('course_age' = 'month') %>%
        select(predictions, course_age) %>%
        tidyr::spread(key = course_age, value = predictions) %>%
        mutate(created_date = min(predicts_df$created_at)) %>%
        bind_cols(course) %>%
        mutate(offer_id = row_offer_id) 
      
      
      temp_predictions_df <<- temp_p_df %>%
        bind_rows(temp_predictions_df)
      
      
      
    } else {
      temp_df <- data.frame(preliminary_royalty_rate = NA,
                            preliminary_completion_payment = NA,
                            preliminary_second_rr = NA,
                            preliminary_second_cp = NA,
                            learning_resource_id = course$learning_resource_id,
                            learning_resource_name = course$learning_resource_name,
                            created_date = Sys.Date(),
                            offer_id = row_offer_id,
                            stringsAsFactors = FALSE)
      
      temp_offers_df <<- bind_rows(temp_offers_df, temp_df)
      
      temp_p_df <- data.frame(
        `1`=NA, `2`=NA, `3`=NA, `4`=NA, `5`=NA, `6`=NA, `7`=NA, `8`=NA, `9`=NA,
        `10`=NA, `11`=NA, `12`=NA, `13`=NA, `14`=NA, `15`=NA, `16`=NA, `17`=NA,
        `18`=NA, `19`=NA, `20`=NA, `21`=NA, `22`=NA, `23`=NA, `24`=NA, `25`=NA,
        `26`=NA, `27`=NA, `28`=NA, `29`=NA, `30`=NA, `31`=NA, `32`=NA,
        `33`=NA, `34`=NA, `35`=NA, `36`=NA)
      
      current_predicts <- as.character(1:36)
      names(temp_p_df) <- current_predicts
      
      temp_p_df <- temp_p_df %>%
        mutate(created_date = Sys.Date()) %>%
        bind_cols(course) %>%
        mutate(offer_id = row_offer_id) 
      
      
      temp_predictions_df <<- temp_p_df %>%
        bind_rows(temp_predictions_df)
      
      errors_df <<- bind_rows(errors_df, course)
      
    } #End of Model Predictions
    
  } #End of Pending Courses
  
  
  temp_predictions_df <<- temp_predictions_df[!is.na(temp_predictions_df$`1`),]
  
  temp_offers_df <<- temp_offers_df[!is.na(temp_offers_df$preliminary_second_rr),]
  return("TRUE")
  
}

msg <- royalty_offers(offer_data)

if(nrow(temp_predictions_df)>0){
  final_predictions <- current_predicts %>% mutate(created_date = as_datetime(created_date)) %>%
    filter(!learning_resource_id %in% temp_predictions_df$learning_resource_id) %>%
    bind_rows(temp_predictions_df) %>%
    filter(!is.na(`1`))
  p_result <- final_predictions
  # Write to snowflake
  letItSnow(p_result, tableName = 'OLD_COURSE_PREDICTIONS_TABLE', overwrite = TRUE, schema = 'UNCERTIFIED')
  
}

if(nrow(temp_offers_df)>0){
  offers_df <- temp_offers_df %>%
    filter(!is.na(preliminary_second_cp)) %>%
    bind_rows(good_offers)
  
  o_result <- offers_df
  # Write to snowflake
  letItSnow(o_result, tableName = 'OLD_COURSE_OFFERS_TABLE', overwrite = TRUE, schema = 'UNCERTIFIED')
  
}

final_msg <- if(nrow(offers_df) > 0 &
                nrow(final_predictions) > 0 &
                msg == "TRUE" &
                !any(is(o_result, "try-error")) &
                !any(is(p_result, "try-error"))){
  "SUCCESS - automated pricing ran for old model"
} else if (nrow(offer_data) < 1 & 
           nrow(temp_predictions_df) == 0 &
           nrow(temp_offers_df) == 0) {
  "Pricing Automation - SUCCESS: No new courses to generate offers for!"
} else {
  "FAILED - automated pricing old model"
}
# send notification
pushover.r(final_msg)
rm(list= c('final_msg', 'temp_offers_df', 'temp_predictions_df', 'cp_model_core',
           'offers_df','o_result', 'p_result', 'msg', 'royalty_offers'))

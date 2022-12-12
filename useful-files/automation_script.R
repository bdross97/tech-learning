###############################################
###############################################
######  THIS VERSION OF THE FILE IS MADE ######
###### SPECIFICALLY FOR IPS FROM THE API ######
###############################################
###############################################

royalty_offers <- function(offer_data){
  #### REMOVE THIS WHEN TOP 100 ARE FIXED ####
  library(stringr)
  library(gower)
  library(cluster)
  #List top 100 slugs/subjects to search incoming content titles for top 100 content refreshes. Will be deprecated when refreshes are linked to old course
  top_100_slugs <- c('react', 'angular', 'csharp', 'react js', 'python', 'comptia', 'dotnet',
                     'aspnet', 'sql', 'kubernetes', 'docker', 'cplusplus', 'linq', 'c sharp', 'hacking', 'javascript',
                     'java', 'aws', 'powershell', 'apache', 'kafka', 'agile', 'devops', 'power bi', 'data analytics', 'aspdotnet',
                     'html', 'jenkins', 'maven', 'azure', ' ng ', 'lfcs', 'terraform', 'patterns', 'azure', 'mvc4', 'cloud computing', 'nodejs',
                     'html5', 'angularjs', 'bash', 'jquery', 'go', 'cqrs', 'asp.net', 'c#','dot net', 'css', 'spring', 'typescript', 'git')
  #Pull in list of master tags updated monthly to reflect new incoming tags that have been assigned to content
  tag_master <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.MASTER_TAGS")
  #Pull in medoids from clustering algorithm training performed locally, these are 45 courses that incoming content is measured against to test similarity to assign new content to a cluster
  medoids <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_AC_MEDOIDS")
  #Pulls in modeling column names stored in snowflake to use to align xgboost column order with incoming data 
  modeling_column_names <- as.character(snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_AC_MODEL_FEATURES ORDER BY col_index")$feat_names)
  all_distinct_tags <- snowflake_query("SELECT 
 lvl1.preflabel as level_1
 ,lvl2.preflabel as level_2
 FROM DVS.CURRENT_STATE.SKILLS_TAGASSIGNMENTS_V2_ASSIGNMENTS b
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl1 on b.level1 = lvl1.id
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl2 on b.level2 = lvl2.id
WHERE level_1 IS NOT NULL")
  
  l_1_tags <- all_distinct_tags %>%
    dplyr::select(level_1) %>%
    mutate(level_1 = tolower(level_1)) %>%
    distinct()
  
  l_2_tags <- all_distinct_tags %>%
    dplyr::select(level_2) %>%
    mutate(level_2 = tolower(level_2)) %>%
    distinct()
  # Model Loading
  # Utilities folder loads snowflake connection functions, S3 connection functions, and pushover notification functions
  source('/home/scripts/utilities.R')
  # DTPLYR loading after dplyr loading to speed up dplyr operations 
  library(dtplyr)
  options(dplyr.summarise.inform = FALSE)

  #Read models from S3 bucket storage
  # XGBoost model to project view time percentage loaded from S3 bucket
  xgb_24_model <- read_s3_object(object = 'xgb_mbo_24_months.RDS', read_fun = readRDS)
  # XGBoost model to project quantile view time group loaded from S3 bucket
  quantile_model <- read_s3_object(object = 'quant_model.RDS', read_fun = readRDS)
  # Royalty revenue table pulled and used for projected revenue and viewership payment calculations
  royalty_revenue <- snowflake_query("SELECT 
                                     date_month as usage_year_month,
                                     seconds AS royalty_revenue
                                     FROM ANALYTICS.UNCERTIFIED.ROYALTY_POOL_PROJECTED")
  platform_size <- snowflake_query("Select *
From UNCERTIFIED.PX_VIDEO_PLATFORM_SIZE")
  # Error checking to see if Platform Size update script completed successfully - should run with model re-training on a monthly cadence
  if(
  platform_size %>%
  filter(usage_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    nrow() < 24
  ){
    for(x in seq(1:3)){
      try(pushoverr::pushover(message = "PLAT SIZE RAN OUT!",
                              user = Sys.getenv("PUSH_USR"), app = Sys.getenv("PUSH_PWD")))
      Sys.sleep(5)
    }
    return("PLAT")
  }
  # Munging royalty revenue data to include only 2 years of advance data projected by revenue projection script
  royalty_revenue <- royalty_revenue %>%
    filter(usage_year_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    arrange(usage_year_month) %>%
    mutate(course_age = row_number()) %>%
    filter(course_age <= 24)
  # Similar error checking from above to ensure enough data exists to project 2 years out
  if(nrow(royalty_revenue)<24){
    for(x in seq(1:3)){
      try(pushover.r("REV PROJECTIONS RAN OUT!"))
      Sys.sleep(5)
    }
    return("REV")
  }
  # Joining both platform size and revenue data for the 2 year projection period
  size_rev <- platform_size %>%
    filter(usage_month >= lubridate::floor_date(Sys.Date(), "month")+months(2)) %>%
    right_join(royalty_revenue, by = c("usage_month"="usage_year_month"))
  
  # New Comp Targets from Snowflake updated by Valerie Winalski
  comp_targets <- snowflake_query('select * from analytics.uncertified.vw_compensation_targets')
  
  # Creation of independent data frames to store projections and offer figures in outside of sourced script
  temp_offers_df <<- data.frame()
  temp_predictions_df <<- data.frame()
  errors_df <<- data.frame()
  
  # Storing unique course title and learning resource ID combinations to iterate over in for loop
  unique_courses <- unique(offer_data[c("course_title", "learning_resource_id")])
  # Initializing for loop sequencing
  for(i in seq(1:nrow(unique_courses))){
    #Get individual course from offer data
    c_df <- offer_data %>%
      filter(course_title == unique_courses[i,]$course_title & learning_resource_id == unique_courses[i,]$learning_resource_id)
    #Check if course title contains any string matches from top 100 string vector
    res <- grep(x = tolower(unique(c_df$course_title)), pattern = paste(top_100_slugs,collapse="|"))
    # Assign variable to be used later on in offer framework if course contains top 100 terminology
    if(length(res) > 0) {
      top_100_variable <- TRUE
    } else {
      top_100_variable <- FALSE
    }
    # Check for multiple tag sets for single course
    # If multiple tagsets exist, execute this logic
    if (nrow(c_df) > 1) {
      # Round path position to be whole number
      c_df <- c_df %>%
        mutate(path_position = round(mean(as.numeric(path_position)),0))
      # Select only LRID and level_1 and level_2 columns
      tag_only <- c_df %>% select(learning_resource_id, level_1, level_2)
      
      # Change untrained tags to "other"
      tag_only <- tag_only %>%
        mutate(level_1 = ifelse(!level_1 %in% l_1_tags$level_1, "other", level_1),
               level_2 = ifelse(!level_2 %in% l_2_tags$level_2, "other", level_2))
      unloadNamespace(ns = 'fastDummies')
      library(fastDummies)
      # Create dummy column set for level 1 and level 2 tags
      tag_only <- dummy_cols(tag_only, select_columns = c('level_1', 'level_2'), remove_selected_columns = TRUE)
      # Sum dummy columns vertically to determine if overlapping tags exist at the L1 or L2 level.
      tag_only <- as.data.table(tag_only)[, lapply(.SD, sum), by = .(learning_resource_id)]
      # Using ifelse logic determine where tag overlap occurs and reassign it a value of 1.
        tag_only <- as.data.frame(tag_only) %>% 
        mutate(across(2:ncol(tag_only), ~ifelse(.x >= 2, 1, .x)))
        
      # Gather master tag column names and determine which tags already exist in the master set for this course
      missing_tags <- colnames(tag_master)[!colnames(tag_master) %in% colnames(tag_only[,2:ncol(tag_only)])]  
      # Create large matrix of dummy columns with 0 values for all non-represented tags
      # This is used for modeling purposes later on.
      tag_only <- cbind(tag_only, data.frame(matrix(ncol = length(missing_tags), nrow = nrow(tag_only), dimnames = list(NULL, missing_tags))))
      tag_only[is.na(tag_only)] <- 0
      # Remove the level 1 and level 2 columns and gather only a single row for multiple tag set courses
      c_df <- c_df %>%
        select(-level_1, -level_2) %>%
        distinct()
      # Join master tag matrix (single row) onto existing course. This now has dummy variables for all tags on the platform
      # This list also accounts for each unique tag within the multiple tag sets present for this content.
      c_df <- c_df %>% left_join(tag_only, by = 'learning_resource_id')
    } else {
      # Reassign tags unseen by model to "other"
      c_df <- c_df %>%
        mutate(level_1 = ifelse(!level_1 %in% l_1_tags$level_1, "other", level_1),
               level_2 = ifelse(!level_2 %in% l_2_tags$level_2, "other", level_2))
      # Perform same logic as before but without accounting for multiple tag sets
      tag_only <- c_df %>% select(learning_resource_id, level_1, level_2)
      unloadNamespace(ns = 'fastDummies')
      library(fastDummies)
      tag_only <- dummy_cols(tag_only, select_columns = c('level_1', 'level_2'), remove_selected_columns = TRUE)
      
      missing_tags <- colnames(tag_master)[!colnames(tag_master) %in% colnames(tag_only[,2:ncol(tag_only)])]  
      tag_only <- cbind(tag_only, data.frame(matrix(ncol = length(missing_tags), nrow = nrow(tag_only), dimnames = list(NULL, missing_tags))))
      tag_only[is.na(tag_only)] <- 0
      c_df <- c_df %>%
        select(-level_1, -level_2) %>%
        distinct()
      c_df <- c_df %>% left_join(tag_only, by = 'learning_resource_id')
    }
    # Function to use medoids from clustering algorithm to assign incoming content a cluster group. 
    predict_pam = function(medoids,newdata){
      # Gather only the relevant columns (tags, duration)
      newdata <- newdata[,-c(1:4)]
      # Text cleaning the column names
      colnames(newdata) <- gsub(colnames(newdata), pattern = ' |&|-', replacement = '.')
      # Subset column names to match medoid data
      missing_tags <- colnames(medoids)[!colnames(medoids) %in% colnames(newdata)]  
      newdata <- cbind(newdata, data.frame(matrix(ncol = length(missing_tags), nrow = nrow(newdata), dimnames = list(NULL, missing_tags))))
      newdata[is.na(newdata)] <- 0
      newdata <- newdata[,colnames(medoids)]
      nclus = nrow(medoids)
      # Daisy function from 'gower' package to determine similarity/dissimilarity of new content compared to medoids
      DM = daisy(rbind(medoids,newdata),metric="gower")
      # Find minimum dissimilarity from matrix for new content - this gives the cluster number the course should be assigned to
      which.min(as.matrix(DM)[-c(1:nclus),1:nclus])
      
    }
    # Apply function to data
    c_df$cluster <- unique(predict_pam(medoids, c_df))
    # Arrange size_rev dataframe by course age for binding
    size_rev <- size_rev %>%
      arrange(course_age)
    # bind columns of course information to size_rev data structure - this creates 24 rows for each projected month in the future
    model_input <- c_df %>%
      cbind('course_age' = size_rev$course_age) %>%
     mutate(average_position = if_else(as.numeric(unique(path_position)) > 5 | as.numeric(unique(path_position)) == 0 | is.na(path_position),
                                   0, switch(EXPR = as.numeric(unique(path_position)), 5, 4, 3, 2, 1))) %>%
      select(-path_position) %>%
      rename('course_name' = 'course_title')
    # Re-structure variables as factors for dummy column creation
    model_input <- model_input %>%
      mutate(average_position = as.factor(average_position),
             course_age = as.character(course_age))
    #This is a bug with the fast dummies package, I'm not sure why it does this but it needs to be in here to work properly
    unloadNamespace(ns = "fastDummies")
    library(fastDummies)
    model_input <- dummy_cols(model_input, select_columns = c('average_position', 'course_age', 'cluster'), remove_selected_columns = TRUE)
    # Text munging column names of data to match modeling input names. 
    colnames(model_input) <- gsub(colnames(model_input), pattern = ' |&|-', replacement = '.')
    # Finding subset of names relevant to incoming course data structure
    sub_names <- modeling_column_names[7:length(modeling_column_names)]
    # Locate the missing features between the new course data and the modeling input columns
    missing_features <- sub_names[!sub_names %in% colnames(model_input[4:ncol(model_input)])] 
    # Bind missing features to incoming data set to be used for modeling
    model_input <- cbind(model_input, data.frame(matrix(ncol = length(missing_features), nrow = nrow(model_input), dimnames = list(NULL, missing_features))))
    model_input[is.na(model_input)] <- 0
    # Find duration in hours by dividing minute duration by 60
    model_input$duration_in_hours <- model_input$duration_in_hours / 60
    # Bind size_rev
    model_input <- cbind(size_rev, model_input)
    
    #Quantile projections
    # Gather single row of model input data (quantile projections are not dependent on time series)
    quant_data <- model_input[1,]
    # Build features missing from incoming data present in quantile model related to tags
    quant_data <- quant_data %>%
      mutate(n_tags_total = rowSums(quant_data[,c(grep("level_1_", colnames(quant_data)))]) + rowSums(quant_data[,c(grep("level_2_", colnames(quant_data)))]),
             n_tags_lvl1 = rowSums(quant_data[,c(grep("level_1_", colnames(quant_data)))]),
             n_tags_lvl2 = rowSums(quant_data[,c(grep("level_2_", colnames(quant_data)))]),
             tag_overlap = ifelse(rowSums(quant_data[,c(grep("level_1_", colnames(quant_data)))]) != rowSums(quant_data[,c(grep("level_2_", colnames(quant_data)))]), 1, 0))
      
    quant_names <- colnames(quant_data)
    #Subset quant data names to match those from algorithm features
    quant_names <- quant_names[quant_names %in% quantile_model$feature_names]  
    quant_data <- quant_data[,quant_names]
    #Reorder columns to match algorithm order
    quant_data <- quant_data[,quantile_model$feature_names]
    #Create model matrix to be input into XGB algorithm
    quant_matrix <- model.matrix(~.+0,data = quant_data) 
    
    #Predict quantile group using incoming data and quantile model
    test_pred <- predict(quantile_model, newdata = quant_matrix)
    # Create transposed data frame to determine most likely predicted quantile group
    test_prediction <- matrix(test_pred, nrow = 5,
                              ncol=length(test_pred)/5) %>%
      t() %>%
      data.frame() %>%
      mutate(max_prob = max.col(., "last") - 1)
    # assign most likely quantile group to incoming course data
    model_input$quant_vt <- as.numeric(test_prediction$max_prob)
    # Create features for overall model input to be used in coures performance projections
    model_input <- model_input %>%
      mutate(n_tags_total = unique(quant_data$n_tags_total),
             n_tags_lvl1 = unique(quant_data$n_tags_lvl1),
             n_tags_lvl2 = unique(quant_data$n_tags_lvl2),
             tag_overlap = unique(quant_data$tag_overlap))
    # Bugged fastdummies lines
    unloadNamespace(ns = "fastDummies")
    library(fastDummies)
    # Create dummy columns for newly created quantile view time column - will replace missing groups in next lines
    model_input <- dummy_cols(model_input, select_columns = c('quant_vt'), remove_selected_columns = TRUE)
    
    #Subset quant data names to match those from algorithm features
    colnames(model_input) <- gsub(colnames(model_input), pattern = ' |&|-', replacement = '.')
    missing_features <- xgb_24_model$feature_names[!xgb_24_model$feature_names %in% colnames(model_input[4:ncol(model_input)])]  
    model_input <- cbind(model_input, data.frame(matrix(ncol = length(missing_features), nrow = nrow(model_input), dimnames = list(NULL, missing_features))))
    model_input[is.na(model_input)] <- 0
    #Reorder columns to match algorithm order
    matrix_model <- model.matrix(~.+0,data = model_input[,-c(1, 3:6)]) 
    matrix_model <- matrix_model[,xgb_24_model$feature_names]
    
    # Add a unique identifier for each offer
    row_offer_id <- uuid::UUIDgenerate(use.time = TRUE)
    
    # Important objects used in determining course offers
    comp_payment_1 <- min(max((unique(model_input$duration_in_hours)+1)*1000, 1500), 4000)
    
    # Create duration in hours in quarter hour increments to match target dataset 
    duration_in_hours <- ifelse(ceiling(unique(model_input$duration_in_hours) * 4) / 4 > 13.00, 
                                13.00, ceiling(unique(model_input$duration_in_hours) * 4) / 4)

    # merge target dataset by duration to find target comp for incoming course
    target <- comp_targets[comp_targets$duration_in_hours == duration_in_hours,][[2]]
    

    #### Model Predictions ####
    # Predict course performance using model over 24 month period
    predicts_df <- try(data.frame(predictions = exp(predict(object = xgb_24_model
                                                        , newdata = matrix_model))
                                  , course_age = seq(1:24)
    ))

    if(!is(predicts_df, 'try-error')){
      # Join royalty revenue by course age to find attributable revenue
      predicts_df <- predicts_df %>%
        left_join(royalty_revenue, by = c("course_age")) %>%
        mutate(attributable_revenue = predictions * royalty_revenue)
      # If top 100 string exists in course title
      if(top_100_variable == TRUE) {
        # Project target earnings over 36 month period using 24 month projections
        # Divide by total attributable revenue to find initial royalty rate
        royalty_rate_1 <-  ((target/36)*24)/sum(predicts_df$attributable_revenue)
        # Manipulate royalty rate calculation to account for rates greater than 10% and less than 6%
        predicts_df <- predicts_df %>%
          mutate(royalty_rate = royalty_rate_1
                 , royalty_rate = ifelse(royalty_rate > .10, .10, royalty_rate)
                 , royalty_rate = ifelse(royalty_rate < .06, .06, royalty_rate)
                 , royalty_pmt = round(royalty_rate * attributable_revenue, 2)
          )
        #Perform same operations for courses that don't match top 100 string
      } else {
        royalty_rate_1 <- ((target/36)*24)/sum(predicts_df$attributable_revenue)
        # Constrain royalty rates to 15% and 6% bounds
        predicts_df <- predicts_df %>%
          mutate(royalty_rate = royalty_rate_1
                 , royalty_rate = ifelse(royalty_rate > .15, .15, royalty_rate)
                 , royalty_rate = ifelse(royalty_rate < .06, .06, royalty_rate)
                 , royalty_pmt = round(royalty_rate * attributable_revenue, 2)
          )
      }
      # Find total royalties and total compensation with completion payment
      total_royalties <- sum(predicts_df$royalty_pmt)
      total_comp = total_royalties + comp_payment_1
      
      # Determine royalty rate for Offer 2
      predicts_df <- predicts_df %>%
        mutate(second_rr = .05
               , second_royalty_pmt = (second_rr/royalty_rate)*royalty_pmt
               , second_cp = round(min((sum(royalty_pmt)-sum(second_royalty_pmt))/2+comp_payment_1
                                       , 10000), -2)
               , created_at = Sys.Date())
      
      # Find second completion payment for 'low-risk' authors
      comp_payment_2 = round(mean(predicts_df$second_cp), 2)
      comp_payment_2 = ifelse(comp_payment_2 < 4500,
                              4500,
                              comp_payment_2)
      
      #Consolidate course offers into dataframe for import into tableau dashboard
      temp_df <- data.frame(preliminary_royalty_rate = round(mean(predicts_df$royalty_rate),3),
                            preliminary_completion_payment = comp_payment_1,
                            preliminary_royalties = sum(predicts_df$royalty_pmt),
                            preliminary_second_rr = mean(predicts_df$second_rr),
                            preliminary_second_cp = comp_payment_2,
                            preliminary_second_royalties = sum(predicts_df$second_royalty_pmt),
                            learning_resource_id = c_df$learning_resource_id,
                            learning_resource_name = c_df$course_title,
                            created_date = unique(predicts_df$created_at),
                            offer_id = row_offer_id,
                            stringsAsFactors = FALSE)
      
      # Bind rows to external dataframe to be written to snowflake
      temp_offers_df <<- bind_rows(temp_offers_df, temp_df)
      #Select single row of course data
      c_df <- c_df[1,]
      # Store all relevant course data for trackign errors in predictions and compensation accuracy
      temp_p_df <- predicts_df %>%
        mutate(predictions = as.numeric(predictions)) %>%
        select(predictions, course_age) %>%
        mutate(created_date = Sys.Date(),
               all_tags = paste0(gsub(gsub(apply(c_df[,c(5:(ncol(c_df) - 1))], 1, function(x) paste(colnames(c_df[,c(5:(ncol(c_df) - 1))])[x > 0], collapse = ', ')), 
                                    pattern = 'level_1_', replacement = 'Level 1: '), pattern = 'level_2_', replacement = 'Level 2: '), sep = ', '),
               n_tags_total = rowSums(c_df[,c(grep("level_1_", colnames(c_df)))]) + rowSums(c_df[,c(grep("level_2_", colnames(c_df)))]),
               lvl_1_tags = rowSums(c_df[,c(grep("level_1_", colnames(c_df)))]),
               lvl_2_tags = rowSums(c_df[,c(grep("level_2_", colnames(c_df)))]),
               tag_overlap = rowSums(c_df[,c(grep("level_1_", colnames(c_df)))]) != rowSums(c_df[,c(grep("level_2_", colnames(c_df)))]),
               cluster = c_df$cluster,
               preliminary_completion_payment = comp_payment_1,
               attributable_revenue = predicts_df$attributable_revenue,
               preliminary_royalty_rate = round(mean(predicts_df$royalty_rate),3),
               royalty_pmt = round(predicts_df$royalty_rate * predicts_df$attributable_revenue, 2),
               target_pmt = (target/36),
               title = c_df$course_title,
               duration = c_df$duration_in_hours / 60,
               path_pos = c_df$path_position,
               offer_id = row_offer_id)

      # Bind rows to external dataframe to be written to snowflake
      temp_predictions_df <<- temp_p_df %>%
        bind_rows(temp_predictions_df)
      


    } else {
      temp_df <- data.frame(preliminary_royalty_rate = NA,
                            preliminary_completion_payment = NA,
                            preliminary_second_rr = NA,
                            preliminary_second_cp = NA,
                            learning_resource_id = c_df$learning_resource_id,
                            learning_resource_name = c_df$learning_resource_name,
                            created_date = Sys.Date(),
                            offer_id = row_offer_id,
                            stringsAsFactors = FALSE)
      
      temp_offers_df <<- bind_rows(temp_offers_df, temp_df)
      
      temp_p_df <- data.frame(
        `1`=NA, `2`=NA, `3`=NA, `4`=NA, `5`=NA, `6`=NA, `7`=NA, `8`=NA, `9`=NA,
        `10`=NA, `11`=NA, `12`=NA, `13`=NA, `14`=NA, `15`=NA, `16`=NA, `17`=NA,
        `18`=NA, `19`=NA, `20`=NA, `21`=NA, `22`=NA, `23`=NA, `24`=NA)
      
      current_predicts <- as.character(1:24)
      names(temp_p_df) <- current_predicts
      
      temp_p_df <- temp_p_df %>%
        mutate(created_date = Sys.Date()) %>%
        bind_cols(c_df) %>%
        mutate(offer_id = row_offer_id) 

      
      temp_predictions_df <<- temp_p_df %>%
        bind_rows(temp_predictions_df)
      
      errors_df <<- bind_rows(errors_df, c_df)

    } #End of Model Predictions
    
  } #End of Pending Courses
  

  # temp_predictions_df <<- temp_predictions_df[!is.na(temp_predictions_df$predictions),]
  temp_predictions_df <<- temp_predictions_df %>%
    filter(!is.na(predictions))

  # temp_offers_df <<- temp_offers_df[!is.na(temp_offers_df$preliminary_second_rr),]
  temp_offers_df <<- temp_offers_df %>%
    filter(!is.na(preliminary_second_rr))
  return("TRUE")
  
}

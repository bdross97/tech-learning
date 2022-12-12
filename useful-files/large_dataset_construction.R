
library(tidyverse)
library(lubridate)
library(data.table)
library(dtplyr)
# library(googlesheets4)
# library(googledrive)
library(fastDummies)
library(gower)
library(cluster)
# Source project utilities
source('/home/scripts/utilities.R')
#Load Medoid data from clustering algorithm
medoids <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_AC_MEDOIDS")
#### usage data #### ----- Remember to omit Free April!
usage_data <- snowflake_query("SELECT 
	  SUM(view_time_hours) as view_time_hours,
	  trunc(start_day_utc, 'mm') as usage_year_month,
	  lower(course_id) as course_id,
	  lower(course_title) as course_title,
	  lower(course_name) as course_name
	FROM source_system.product.clipview_all
	WHERE trunc(start_day_utc, 'mm') BETWEEN (
	SELECT MAX(USAGE_YEAR_MONTH)
	FROM ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA
	) AND trunc(current_date(), 'mm')
	GROUP BY
	  trunc(start_day_utc, 'mm'),
	  course_id,
	  course_name,
	  course_title") %>%
  # rename and change format of usage year month column
  mutate(usage_year_month = as.Date(usage_year_month)) %>%
  # remove free april, need to scale this to be reliable
  filter(!usage_year_month %in% c(as.Date("2020-04-01"), as.Date("2021-04-01"), as.Date("2022-04-01"))) # Exclude Free April
# Use lubridate to impute quarter of year using usage year month
usage_data$q <- lubridate::quarter(usage_data$usage_year_month, with_year = FALSE)

#Combine data points of courses that received refreshes and a new name
course_replacements <- snowflake_query("SELECT 
	b.slug as course_name
	, b.id
	, b.commissioneddate AS course_commissioned_date__c
  , b.retiredreplacementcourseid
  , b.retireddate
  , b.retiredreason
  , b.releasedate
  , c.slug
  , d.published_date
FROM ANALYTICS.CERTIFIED.PX_CONTENT_TOOLS_COURSES b
LEFT JOIN ANALYTICS.CERTIFIED.PX_CONTENT_TOOLS_COURSES c ON b.retiredreplacementcourseid = c.id
LEFT JOIN analytics.uncertified.px_course_library_with_tags d ON b.slug = d.course_name") 
# Include most recent information on courses that were renamed or refreshed by joining course replacements data
usage_data <- usage_data %>%
  left_join(course_replacements, by = c('course_name')) %>% #name is newest course name
  mutate(course_id = if_else(is.na(retiredreplacementcourseid),course_id,slug))

#Select only pertinent columns for usage data and gather view time in hours
usage_data <- usage_data %>%
  select(course_id
         , course_title
         , course_name
         , usage_year_month
         , view_time_hours
         , published_date
         , q) %>%
  group_by(course_id
           , course_name
           , course_title
           , usage_year_month
           , q) %>%
  summarise(view_time_hours = sum(view_time_hours)
            , published_date = min(published_date,na.rm=TRUE))

#### Course Library Data ####
course_library_with_tags <- snowflake_query("SELECT DISTINCT
  a.course_name
 ,c.course_duration_in_seconds as duration_in_hours
 ,lvl1.preflabel as level_1
 ,lvl2.preflabel as level_2
FROM analytics.uncertified.px_course_library_with_tags a
LEFT JOIN DVS.CURRENT_STATE.SKILLS_TAGASSIGNMENTS_V2_ASSIGNMENTS b ON a.course_id = b.contentid
LEFT JOIN source_system.product.course c ON c.course_id = b.contentid
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl1 on b.level1 = lvl1.id
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl2 on b.level2 = lvl2.id
WHERE a.PUBLISHED_DATE IS NOT NULL AND level_1 IS NOT NULL") %>%
  mutate(level_1 = tolower(level_1),
         level_2 = tolower(level_2),
         #create duration in hours using calculation below
         duration_in_hours = ceiling(((duration_in_hours / 60) / 60) * 4)/4)

cl_tags <- course_library_with_tags %>%
  select(course_name, level_1, level_2)

#Generate dummy columns for level 1 and level 2 tags
cl_tags <- fastDummies::dummy_cols(cl_tags, select_columns = c('level_1', 'level_2'), 
                                   remove_selected_columns = TRUE)
#Insert any missing columns from master tag list created by file 'master_tag_list.R'
tag_master <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.MASTER_TAGS")
missing_tags <- colnames(tag_master)[!colnames(tag_master) %in% colnames(cl_tags[,2:ncol(cl_tags)])]  
cl_tags <- cbind(cl_tags, data.frame(matrix(ncol = length(missing_tags), nrow = nrow(cl_tags), dimnames = list(NULL, missing_tags))))
cl_tags[is.na(cl_tags)] <- 0
#apply summation to gather only unique tags from level 1 and level 2 courses.
cl_tags <- as.data.table(cl_tags)[, lapply(.SD, sum), by = .(course_name)]
cl_tags <- as.data.frame(cl_tags) %>% 
  mutate(across(2:ncol(cl_tags), ~ifelse(.x >= 2, 1, .x)))
# Text cleaning columns for clustering algorithm application
colnames(cl_tags) <- gsub(colnames(cl_tags), pattern = ' |&|-', replacement = '.')


#Generating clusters by applying daisy function using medoids list
predict_pam = function(medoids,newdata){
  newdata <- newdata[,-1]
  newnames <- colnames(newdata)[colnames(newdata) %in% colnames(medoids)] 
  missing_names <- colnames(medoids)[!colnames(medoids) %in% colnames(newdata)] 
  newdata <- newdata[,newnames]
  newdata <- cbind(newdata, data.frame(matrix(ncol = length(missing_names), nrow = nrow(newdata), dimnames = list(NULL, missing_names))))
  nclus = nrow(medoids)
  clust_list <- list()
  for(i in 1:nrow(newdata)){
    DM = daisy(rbind(medoids,newdata[i,]),metric="gower")
    clust_list[i] <-unique(which.min(as.matrix(DM)[-c(1:nclus),1:nclus]))
  }
  return(clust_list)
}
# Apply function to new training data
cl_tags$cluster <- predict_pam(medoids, cl_tags)

cl_duration <- course_library_with_tags %>%
  select(course_name, duration_in_hours)

# Join Usage Data with Course Library Data
usage_data <- usage_data %>%
  left_join(cl_tags, by = c('course_name')) %>%
  left_join(cl_duration, by = c('course_name')) %>%
  filter(as.Date(published_date) >= as.Date('2015-01-01'), #no need to have courses older than 2015
         !is.na(`level_1_algorithms...mathematics`)) %>%
  distinct()


#### Path Data ####
path_position_data <- snowflake_query("
                                  SELECT
                                  pathurlslug as path_title
                                  , courseid as course_name
                                  , trunc(pathcreatedat, 'mm') as path_created_date
                                  , pathsection
                                  , courseorderinsection
                                  FROM ANALYTICS.CERTIFIED.PX_PATH_COURSES_AND_ASSESSMENTS_FULL") %>%
  group_by(path_title, path_created_date) %>%
  arrange(pathsection, courseorderinsection, .by_group = TRUE) %>%
  mutate(pathsection = as.integer(pathsection),
         courseorderinsection = as.integer(courseorderinsection),
         new_position = recode(courseorderinsection, 5, 4, 3, 2, 1, .default = 0)) %>%
  ungroup()

####Platform size ####
platform_size <- snowflake_query("Select *
From UNCERTIFIED.PX_VIDEO_PLATFORM_SIZE")
# Fixing platform size historical data thats missing - need to include this in running script
plat_history <- snowflake_query('SELECT MONTH, PLATFORM_SIZE FROM UNCERTIFIED.PX_VIDEO_PLATFORM_SIZE_FORECAST')
# Left join platform size and platform history
platform_size <- plat_history %>%
  left_join(platform_size, by = c('month' = 'usage_month')) 
# consolidate into a singular dataframe by merging missing months between the two dataframes 
platform_size <- platform_size %>%
  mutate(platform_size.x = ifelse(is.na(platform_size.x), platform_size.y, platform_size.x)) %>%
  select(-platform_size.y) %>%
  rename('platform_size' = 'platform_size.x',
         'usage_month' = 'month')


# Combine Initial Data Queries
aggregate_data <- usage_data %>%
  select(course_title, course_name, usage_year_month) %>%
  left_join(path_position_data, by = "course_name") %>%
  filter(as.Date(path_created_date) < as.Date(usage_year_month),
         !is.na(path_title)) %>%
  group_by(course_title, usage_year_month) %>%
  summarise(average_position = mean(new_position)) %>%
  right_join(usage_data, by = c("course_title", "usage_year_month")) %>%
  mutate(average_position = if_else(is.na(average_position), 0, average_position)) %>%
  left_join(platform_size, by = c("usage_year_month" = "usage_month")) %>%
  select(-course_id) %>%
  group_by(across(c(-view_time_hours))) %>%
  summarise(view_time_hours = sum(view_time_hours))

#Initial Data Cleaning for EDA
cleaned_data <- aggregate_data %>%
  #Filter out all data points with an age greater than 36
  mutate(usage_year_month =  format(usage_year_month, "%Y-%m-01")) %>%
  group_by(usage_year_month) %>%
  mutate(course_age = as.integer(lubridate::interval(published_date
                                                     , usage_year_month) %/% months(1)),
         total_view_time = sum(view_time_hours, na.rm = TRUE),
         view_time_perc = view_time_hours/total_view_time) %>%
  ungroup() %>%
  distinct() %>%
  filter(course_age <= 24 & course_age >= 0) %>%
  arrange(course_name, course_age)

# Filtering out partnership content 
partnerships <- snowflake_query("SELECT name, library_course_id__c, partnership_type__c FROM source_system.salesforce.opportunity 
                                WHERE partnership_type__c = 'PS Ingestion'")
# setting strings for conference specific courses to remove later on 
conf_updates <- c('%ng-conf%','%front-utah%','%snowforce-io%','%snowforce-2020%', '%snowforce-2019%',
                  '%microsoft-ignite%', '%sharepoint-conf%', '%swiftfest-boston%',
                  '%women-analytics%', '%microsoft-ignite%', '%microsoft-azureai%',
                  '%codemash%', '%dev-intersection%', '%bsides-huntsville%',
                  '%big-data-ldn%','%bsides-jax%','%angular-denver%','%droidcon-sf%',
                  '%droidcon-nyc%', '%droidcon-boston%', '%that-conference%', 'conference')


# Remove all partnership and conference content from training set
cleaned_data <- cleaned_data %>%
  filter(!course_name %in% partnerships$library_course_id__c)

setDT(cleaned_data)

cleaned_data <- cleaned_data[,"conference" := if_else(grepl(paste(conf_updates, collapse = '|'), course_name), 1, 0)] %>%
  filter(conference == 0, 
         usage_year_month != format(Sys.Date(),"%Y-%m-01")) %>%
  select(-conference, -total_view_time, -view_time_hours)

cleaned_data <- as.data.frame(cleaned_data)

cleaned_data$average_position <- round(cleaned_data$average_position, 0)
# Create dummy columns for cluster average position course age and quarter
cleaned_data <- fastDummies::dummy_cols(cleaned_data, select_columns = c('cluster', 'average_position', 'course_age', 'q'), 
                                        remove_selected_columns = TRUE)

cleaned_data <- cleaned_data %>%
  select("course_name",
         "course_title",
         "usage_year_month", 
         "published_date", 
         "duration_in_hours", 
         "platform_size", 
         "view_time_perc", 
         grep("level_1_", colnames(cleaned_data)), 
         grep("level_2_", colnames(cleaned_data)),
         grep("cluster_", colnames(cleaned_data)),
         grep("average_position", colnames(cleaned_data)),
         grep("course_age_", colnames(cleaned_data)),
         grep("q_", colnames(cleaned_data)))


# Adding features related to tags and number of level 1 and 2 tags
cleaned_data <- cleaned_data %>%
  ungroup() %>%
  mutate(tag_overlap = ifelse(rowSums(cleaned_data[,c(grep("level_1_", colnames(cleaned_data)))]) != rowSums(cleaned_data[,c(grep("level_2_", colnames(cleaned_data)))]), 1, 0),
         n_tags_total = rowSums(cleaned_data[,c(grep("level_1_", colnames(cleaned_data)))]) + rowSums(cleaned_data[,c(grep("level_2_", colnames(cleaned_data)))]),
         n_tags_lvl1 = rowSums(cleaned_data[,c(grep("level_1_", colnames(cleaned_data)))]),
         n_tags_lvl2 = rowSums(cleaned_data[,c(grep("level_2_", colnames(cleaned_data)))]))
# Preparing quantile data for quantile grouping 
quantile_breaks <- snowflake_query('WITH cte1 AS (
                                   SELECT course_title,
                                    MAX(quant_vt) as quant_vt,
                                    SUM(view_time_perc) as total_vt
                                   FROM ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA
                                   GROUP BY course_title, quant_vt
                                   ) 
                                   SELECT MAX(total_vt) as group_max,
                                   quant_vt
                                   FROM cte1 
                                   GROUP BY quant_vt
                                   ORDER BY quant_vt')

hist_totals <- snowflake_query('SELECT course_title,
                                    SUM(view_time_perc) as total_vt
                                   FROM ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA
                                   GROUP BY course_title')

quantile_courses <- cleaned_data %>%
  group_by(course_title) %>%
  summarise(total_vt_pct = sum(view_time_perc)) %>%
  ungroup() %>%
  left_join(hist_totals, on = 'course_title') %>%
  group_by(course_title) %>%
  summarise(total_vt_pct = total_vt_pct + total_vt) %>%
  mutate(quant_vt = as.integer(cut(total_vt_pct, breaks = c(0,quantile_breaks$group_max), 
                                   include.lowest = TRUE, labels = 1:5)) - 1) %>%
  dplyr::select(-total_vt_pct)
# Joining quantile groups for each course to use in training quantile model
cleaned_data <- cleaned_data %>% left_join(quantile_courses, by = 'course_title') %>% arrange(course_title, usage_year_month) %>% distinct()

existing_columns <- tolower(snowflake_query("SHOW COLUMNS IN ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA")$column_name)

missing_columns <- existing_columns[!existing_columns %in% colnames(cleaned_data)]  
cleaned_data <- cbind(cleaned_data, data.frame(matrix(ncol = length(missing_columns), nrow = nrow(cleaned_data), dimnames = list(NULL, missing_columns))))
cleaned_data[is.na(cleaned_data)] <- 0
cleaned_data <- cleaned_data[existing_columns]
# # Drop then replace table for author comp training dataset
# snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA")
# # For looping to handle large amounts of data writing to Snowflake
# n <- 10000
# nr <- nrow(cleaned_data)
# x <- split(cleaned_data, rep(1:ceiling(nr/n), each=n, length.out=nr))
# #Write first section to create table 
# letItSnow(x[[1]], schema = "UNCERTIFIED", tableName = 'AUTHOR_COMP_PROD_TRAIN_DATA', overwrite = TRUE)
#For loop through the remaining data and append it to the existing table
# for (i in 2:length(x)) {
#   data <- x[[i]]
  letItSnow(cleaned_data, tableName = "BR_PATCH_FIX", overwrite = TRUE)
  snowflake_query("INSERT INTO ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA SELECT * FROM ANALYTICS.SANDBOX.BR_PATCH_FIX")
  snowflake_query("DROP TABLE ANALYTICS.SANDBOX.BR_PATCH_FIX")
#   print('Success!')
# }


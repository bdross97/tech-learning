library(caret)
library(data.table)
library(xgboost)
library(mlr3)
source('/home/scripts/utilities.R')
#Reading dataset from file 'production_dataset_build.R'
model_dataset <- snowflake_query('SELECT * FROM ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA')

#Remove outliers from view time predictions (bottom and top 5th percentile)
final_model <- model_dataset %>%
  filter(view_time_perc != 0) %>%
  mutate(log_view_time_perc = log(view_time_perc),
         quant_vt = as.factor(quant_vt))
#Set seed for reproducibility
set.seed(42069)
#Randomly sample on course name (ensures all course data is complete between sets)
course_list <- unique(final_model$course_title)
#Train-test split
train_size <- floor(0.7 * length(course_list))
train_ind <- sample(seq_len(length(course_list)), size = train_size)
#Splitting train and test sets by index
train_data <- data.frame('course_title' = course_list[train_ind])
test_data <- data.frame('course_title' = course_list[-train_ind])
#Gather all features for train and test sets
train_data <- train_data %>%
  left_join(final_model, by = 'course_title') %>%
  na.omit()
test_data <- test_data %>%
  left_join(final_model, by = 'course_title') %>%
  na.omit()

#Target extraction
train_targets <- train_data$log_view_time_perc
#Clean train data, now scaled so that code isn't reliant on indices
cleaned_train <- train_data[,-c(grep("course_name|course_title|usage_year_month|published_date|platform_size|log_view_time_perc|view_time_perc", colnames(train_data)))]
setDT(cleaned_train)
#Creation of model matrix with redundant columns removed
tr_matrix <- model.matrix(~.+0,data = cleaned_train) 
#Data table creation for xgb.Matrix formatting
dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)

#Same process applied to test data to ensure early stopping is possible through watchlist

test_targets <- test_data$log_view_time_perc

cleaned_test <- test_data[,-c(grep("course_name|course_title|usage_year_month|published_date|platform_size|log_view_time_perc|view_time_perc", colnames(test_data)))]
setDT(cleaned_test)

test_matrix <- model.matrix(~.+0,data = cleaned_test) 
dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)

# Read hyperparameters from tuning script output
hyper_params <- read_s3_object(object = 'best_parameters_prod_model.RDS', read_fun = readRDS)

#Parameters obtained from local machine training
xgb_model <- xgb.train(
  eta = hyper_params$eta,
  gamma = hyper_params$gamma,
  max_depth = hyper_params$max_depth,
  min_child_weight = hyper_params$min_child_weight,
  subsample = hyper_params$subsample,
  colsample_bytree = hyper_params$colsample_bytree,
  max_delta_step = hyper_params$max_delta_step,
  booster = 'gbtree',
  objective = "reg:squarederror",
  #Maximum number of available threads to train with.
  nthread = hyper_params$nthread,
  eval_metric = 'rmse',
  early_stopping_rounds = 10,
  verbose = TRUE,
  watchlist = list(train = dtrain, eval = dtest),
  data = dtrain,
  nrounds = hyper_params$nrounds,
  print_every_n = 100)

#Save model object and send pushover notification
# saveRDS(xgb_model,file.path("~/SageMaker/efs/author_compensation/video/video_comp_model/Model Storage/xgb_mbo_24_months.RDS"))
# Write model object to s3 bucket
write_s3_rds(object = xgb_model, filename = 'xgb_mbo_24_months.RDS')
# Send pushover notification to relay s3 contents after training
pushover.r(as.character(list_s3_objects()))


rmse_metrics <- data.frame(train_rmse = min(xgb_model$evaluation_log$train_rmse), eval_rmse = min(xgb_model$evaluation_log$eval_rmse),
                           best_iteration = xgb_model$best_iteration,
                           train_date = Sys.Date())
hist_rmse <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_EVAL_METRICS")
hist_rmse <- rbind(hist_rmse, rmse_metrics)
snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.BR_EVAL_METRICS")
letItSnow(hist_rmse, schema = "UNCERTIFIED", tableName = "BR_EVAL_METRICS", overwrite = TRUE)

feature_importance <- data.frame(xgb.importance(model = xgb_model))
melted_importance <- data.frame(melt(feature_importance, id.vars = "Feature"), train_date = Sys.Date())
colnames(melted_importance) <- tolower(colnames(melted_importance))

historical_importance <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_FEATURE_IMPORTANCE")
historical_importance <- rbind(historical_importance, melted_importance)

snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.BR_FEATURE_IMPORTANCE")
letItSnow(historical_importance, schema = "UNCERTIFIED",  tableName = "BR_FEATURE_IMPORTANCE", overwrite = TRUE)


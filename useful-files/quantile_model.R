source('/home/scripts/utilities.R')
library(xgboost)
library(dtplyr)
library(caret)
# library(dummies)
# library(multiROC)
model_data <- snowflake_query('SELECT * FROM ANALYTICS.UNCERTIFIED.AUTHOR_COMP_PROD_TRAIN_DATA')

# Gather unique list of courses and perform train-test split on training data
c_list <- unique(model_data$course_title)
train_size <- floor(0.7 * length(c_list))
train_ind <- sample(seq_len(length(c_list)), size = train_size)

train_data <- data.frame('course_title' = c_list[train_ind])
test_data <- data.frame('course_title' = c_list[-train_ind])
# Remove course age from data as well as other date-related columns 
quant_class <- model_data[,-c(grep("course_age", colnames(model_data)))]

quant_class <- quant_class %>%
  ungroup() %>%
  select(-q_1, -q_2, -q_3, -q_4, -published_date, -usage_year_month, -view_time_perc)
# Split data by training and test splits
xgb_train_data <- train_data %>%
  left_join(quant_class, by = 'course_title') %>%
  na.omit() %>%
  dplyr::select(-course_title, -course_name)
xgb_test_data <- test_data %>%
  left_join(quant_class, by = 'course_title') %>%
  na.omit() %>%
  dplyr::select(-course_title, -course_name)
# Remove target variable and create data to use in training model to project quantile group of courses
test_matrix <- as.matrix(xgb_test_data[,-c(grep("quant_vt", colnames(xgb_test_data)))])
test_lab <- xgb_test_data$quant_vt
val_mat <- xgb.DMatrix(data = test_matrix, label = test_lab)

train_matrix <- as.matrix(xgb_train_data[,-c(grep("quant_vt", colnames(xgb_test_data)))])
train_targets <- xgb_train_data$quant_vt
train_matrix <- xgb.DMatrix(data = train_matrix, label = train_targets)
# Gather number of classes based on quantile grouping split
numberOfClasses <-  length(unique(model_data$quant_vt))
# Set parameters for multi-class xgboost model
xgb_params <- list("objective" = "multi:softprob",
                   "eval_metric" = "mlogloss",
                   "num_class" = numberOfClasses,
                   "nthread" = 6)
#Train model
quant_mod <- xgb.train(params = xgb_params,
                       data = train_matrix,
                       nrounds = 250)
# Predict model on test dataset
test_pred <- predict(quant_mod, newdata = test_matrix)
# Determine overall accuracy of predictions 
test_prediction <- matrix(test_pred, nrow = numberOfClasses,
                          ncol=length(test_pred)/numberOfClasses) %>%
  t() %>%
  data.frame() %>%
  mutate(label = test_lab,
         max_prob = max.col(., "last") - 1)
# Display confusion matrix to determine accuracy across quantile groups
c_mat <- confusionMatrix(factor(test_prediction$max_prob),
                factor(test_prediction$label),
                mode = "everything")

# saveRDS(quant_mod, '~/SageMaker/efs/author_compensation/video/video_comp_model/Model Storage/quantile_xgb.RDS')

# write_s3_rds(object = xgb_24,filename = 'xgb_model.RDS')
write_s3_rds(object = quant_mod, filename = 'quant_model.RDS')
# # Send pushover notification relaying contents of s3 bucket
# pushover.r(as.character(list_s3_objects()))

feature_importance <- data.frame(xgb.importance(model = quant_mod))
melted_importance <- data.frame(melt(feature_importance, id.vars = "Feature"), train_date = Sys.Date())
colnames(melted_importance) <- tolower(colnames(melted_importance))

historical_importance <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_FEATURE_IMPORTANCE")
historical_importance <- rbind(historical_importance, melted_importance)

snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_FEATURE_IMPORTANCE")
letItSnow(historical_importance, schema = "UNCERTIFIED",  tableName = "BR_QUANT_MOD_FEATURE_IMPORTANCE", overwrite = TRUE)



overall_acc <- data.frame('Measure' = rownames(data.frame(c_mat[3])), 'Value' = c_mat[3]$overall,  train_date = Sys.Date(), row.names = NULL)
colnames(overall_acc) <- tolower(colnames(overall_acc))
historical_acc <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_ACCURACY")
historical_acc <- rbind(historical_acc, overall_acc)

snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_ACCURACY")
letItSnow(historical_acc, schema = "UNCERTIFIED",  tableName = "BR_QUANT_MOD_ACCURACY", overwrite = TRUE)


statistics <- data.frame('Measure' = rownames(data.frame(c_mat[4])), c_mat[4]$byClass, train_date = Sys.Date(), row.names = NULL)
statistics <- melt(statistics, id.vars = c('Measure', 'train_date'))
colnames(statistics) <- tolower(colnames(statistics))
historical_stats <- snowflake_query("SELECT * FROM ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_STATISTICS")
historical_stats <- rbind(historical_stats, statistics)
historical_stats$variable <- tolower(historical_stats$variable)
snowflake_query("DROP TABLE ANALYTICS.UNCERTIFIED.BR_QUANT_MOD_STATISTICS")
letItSnow(historical_stats, schema = "UNCERTIFIED",  tableName = "BR_QUANT_MOD_STATISTICS", overwrite = TRUE)


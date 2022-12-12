rm(list=ls())
gc()

library(caret)
library(caretEnsemble)
library(xgboost)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
model_dataset <- fread('/Users/brayden-ross/Desktop/Content Strategy - Git Clones/content_strategy/brayden_ross/author_comp_modeling/updated_model_data.csv')#Remove outliers from view time predictions (bottom and top 5th percentile)
final_model <- model_dataset %>%
  filter(view_time_perc != 0) %>%
  mutate(log_view_time_perc = log(view_time_perc),
         quant_vt = as.factor(quant_vt))

set.seed(42069)
course_list <- unique(final_model$course_name)
#Train-test split
train_size <- floor(0.7 * length(course_list))
train_ind <- sample(seq_len(length(course_list)), size = train_size)

train_data <- data.frame('course_name' = course_list[train_ind])
test_data <- data.frame('course_name' = course_list[-train_ind])

train_data <- train_data %>%
  left_join(final_model, by = 'course_name') %>%
  na.omit()
test_data <- test_data %>%
  left_join(final_model, by = 'course_name') %>%
  na.omit()

setDT(train_data)

train_targets <- train_data$log_view_time_perc

tr_matrix <- model.matrix(~.+0,data = train_data[,-c(1:3, 6, 224)]) 
dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)
group_fit_control <- trainControl(method = 'cv', index = createFolds(y = train_data$course_name, k = 10),
                                  verboseIter = TRUE, savePredictions = "final")


setDT(test_data)

test_targets <- test_data$log_view_time_perc

test_matrix <- model.matrix(~.+0,data = test_data[,-c(1:3, 6, 224)]) 
dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)

best_params <- readRDS('~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/xgb_mbo_24_params.RDS')[-10]
xgb_grid <- expand.grid(nrounds = best_params$nrounds,
                        max_depth = best_params$max_depth,
                        eta = best_params$eta,
                        gamma = best_params$gamma,
                        colsample_bytree = best_params$colsample_bytree,
                        min_child_weight = best_params$min_child_weight,
                        subsample = best_params$subsample)

unregister_dopar <- function() {
  env <- foreach:::.foreachGlobals
  rm(list=ls(name=env), pos=env)
}
unregister_dopar()

model_list <- caretList(
  x = tr_matrix, 
  y = train_data$log_view_time_perc,
  trControl = group_fit_control,
  metric = 'RMSE',
  tuneList = list(
    xgbTree = caretModelSpec(method="xgbTree", tuneGrid = xgb_grid),
    ols=caretModelSpec(method="lm")
  )
)
ensemble_method <- caretEnsemble(
  model_list,
  metric = 'RMSE',
  trControl = trainControl(number = 10, method = 'cv')
)
summary(ensemble_method)
saveRDS(ensemble_method, '~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/ensemble_tuned.RDS')

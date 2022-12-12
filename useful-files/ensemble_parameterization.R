rm(list=ls())
gc()

library(caret)
library(caretEnsemble)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
new_tax_train <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_training_data.csv')
clusters <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/clustering_courses.csv')
new_tax_train <- new_tax_train %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster)) %>%
  na.omit()
set.seed(42069)
group_folds <- readRDS(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"/video/video_comp_model/indexing_kfolds.RDS"))
group_fit_control <- trainControl(method = 'cv', index = group_folds, verboseIter = TRUE, allowParallel = TRUE, savePredictions = "final")
xgbTreeGrid <- expand.grid(nrounds = 543, max_depth = 6, eta = 0.2875,  subsample = 0.75, colsample_bytree = 1, min_child_weight = 1, gamma = 0)
setDT(new_tax_train)
tr_matrix <- model.matrix(~.+0,data = new_tax_train[,-c(1:3, 14)]) 


model_list <- caretList(
  x = tr_matrix, 
  y = new_tax_train$log_view_time_perc,
  trControl = group_fit_control,
  metric = 'RMSE',
  tuneList = list(
    glm=caretModelSpec(method="glm", family = 'Gamma'),
    xgbTree = caretModelSpec(method="xgbTree",  tuneGrid = xgbTreeGrid),
    ols = caretModelSpec(method = 'ols')
  )
)

saveRDS(model_list,file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"video/video_comp_model/new_ensemble_model.RDS"))

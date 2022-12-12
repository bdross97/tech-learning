rm(list=ls())
gc()
.rs.restartR()

library(caret)
library(data.table)
library(xgboost)
library(mlr3)
# library(parallel)
# library(doMC)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
train_data <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_training_data.csv')
clusters <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/clusters.csv')
train_data <- train_data %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster)) %>%
  na.omit()
setDT(train_data)

train_targets <- train_data$log_view_time_perc

tr_matrix <- model.matrix(~.+0,data = train_data[,-c(1:3, 14)]) 

dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)
set.seed(42069)
test_data <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_testing_data.csv')
test_data <- test_data %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster)) %>%
  na.omit()
setDT(test_data)
test_targets <- test_data$log_view_time_perc
test_matrix <- model.matrix(~.+0,data = test_data[,-c(1:3,14)])
dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)
hyper_grid <- expand.grid(max_depth = 6, eta = seq(.20, .35, .0125))
options(scipen = 999)
xgb_train_rmse <- list()
xgb_test_rmse <- list()
# doMC::registerDoMC(cores = 4)
for (j in 1:nrow(hyper_grid)) {
  set.seed(123)
  cat(j, "\n")
  print(paste0('ETA: ', hyper_grid$eta[j]))
  m_xgb_untuned <- xgb.cv(
    data = dtrain,
    nrounds = 1000,
    objective = "reg:squarederror",
    eval_metric = 'mae',
    early_stopping_rounds = 3,
    nfold = 5,
    max_depth = hyper_grid$max_depth[j],
    eta = hyper_grid$eta[j],
    print_every_n = 50
  )
  
  xgb_train_rmse[j] <- m_xgb_untuned$evaluation_log$train_mae_mean[m_xgb_untuned$best_iteration]
  xgb_test_rmse[j] <- m_xgb_untuned$evaluation_log$test_mae_mean[m_xgb_untuned$best_iteration]

}
best_eta <- hyper_grid[which.min(xgb_test_rmse), ]$eta
best_depth <- hyper_grid[which.min(xgb_test_rmse), ]$max_depth
pushover.r(paste0('XGB Model Finished Tuning! Best Parameters: ETA = ', best_eta, ', max_depth = ', best_depth))
best_params <- hyper_grid[which.min(xgb_test_rmse), ]
#Max Depth = 4 ETA = 0.3
#Need to implement regularization or gamma metric to avoid overfitting and reduce model complexity
xgb_model <- xgb.train(data = dtrain, nrounds = 3000, objective = "reg:squarederror", nfold = 10, subsample = 0.75, eval_metric = 'rmse', eval_metric = 'mae',
                       eta = best_params$eta, early_stopping_rounds = 10,  print_every_n = 25, watchlist = list(val=dtest,train=dtrain), 
                       max_depth = best_params$max_depth)

ggplot(data = xgb_model$evaluation_log) + geom_line(aes(x = iter, y = val_mae),  color = 'red') + geom_line(aes(x = iter, y = train_mae), color = 'green') + 
  geom_vline(xintercept = xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_mae),]$iter, linetype = 'dashed', alpha = 0.5) +
  labs(title = 'MAE Evaluation Log', 
       subtitle = paste0('Best Iteration: ', xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_mae),]$iter))

xgb_model <- xgboost(data = dtrain, max_depth = best_params$max_depth, eta = best_params$eta, nfold = 10, subsample = 0.75,
                     nrounds = xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_mae),]$iter, print_every_n = 50)

saveRDS(xgb_model,file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"video/video_comp_model/new_taxonomy_xgboost_model.RDS"))
pushover.r('XGB Model Finished Running!') 

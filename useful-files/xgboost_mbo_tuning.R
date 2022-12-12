rm(list=ls())
gc()
.rs.restartR()

library(caret)
library(data.table)
library(xgboost)
library(mlr3)
library(ggplot2)
library(mlrMBO)
library(ParamHelpers)
library(DiceKriging)
library(smoof)
library(kableExtra)
library(scatterplot3d)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
model_dataset <- read.csv('/Users/brayden-ross/Desktop/Content Strategy - Git Clones/content_strategy/brayden_ross/author_comp_modeling/modeling_dataset_final.csv')
#Remove outliers from view time predictions (bottom and top 5th percentile)
final_model <- model_dataset %>%
  filter(view_time_perc != 0) %>%
  mutate(log_view_time_perc = log(view_time_perc))



# model_matrix_new <- cbind(model_matrix_new, final_model[,c(1,133)])
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

tr_matrix <- model.matrix(~.+0,data = train_data[,-c(1:3, 6, 215)]) 
dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)


setDT(test_data)

test_targets <- test_data$log_view_time_perc

test_matrix <- model.matrix(~.+0,data = test_data[,-c(1:3, 6, 215)]) 
dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)
# hyper_grid <- expand.grid(max_depth = c(6, 8, 10, 12), eta = seq(0.1, .35, 0.02125))
# 
# xgb_train_rmse <- list()
# xgb_test_rmse <- list()

####New Model Based Optimization Search Method####
obj.fun <- makeSingleObjectiveFunction(
  name = "xgb_cv_bayes",
  fn =   function(x){
    set.seed(42)
    cv <- xgb.cv(params = list(
      booster          = "gbtree",
      eta              = x["eta"],
      max_depth        = x["max_depth"],
      min_child_weight = x["min_child_weight"],
      gamma            = x["gamma"],
      subsample        = x["subsample"],
      colsample_bytree = x["colsample_bytree"],
      max_delta_step   = x["max_delta_step"],
      objective        = 'reg:squarederror', 
      eval_metric     = 'rmse'),
      tree_method = 'auto',
      data = dtrain, 
      nround = 2000,
      nthread = 6,
      nfold =  5,
      prediction = FALSE,
      showsd = TRUE,
      early_stopping_rounds = 50, # If evaluation metric does not improve on out-of-fold sample for n rounds, stop
      verbose = 1,
      print_every_n = 125)
    
    cv$evaluation_log %>% pull(4) %>% min  ## column 4 is the eval metric
  },
  par.set = makeParamSet(
    makeNumericParam("eta",                    lower = 0.09, upper = 0.15),
    makeNumericParam("gamma",                  lower = 1,     upper = 3),
    makeIntegerParam("max_depth",              lower= 14,      upper = 18),
    makeIntegerParam("min_child_weight",       lower= 250,    upper = 350),
    makeNumericParam("subsample",              lower = 0.70,  upper = .8),
    makeNumericParam("colsample_bytree",       lower = 0.70,  upper = .8),
    makeNumericParam("max_delta_step",         lower = 0,     upper = 5)
  ),
  minimize = TRUE
)
do_bayes <- function(n_design = NULL, opt_steps = NULL, of = obj.fun, seed = 42069) {
  set.seed(seed)
  
  des <- generateDesign(n=n_design,
                        par.set = getParamSet(of),
                        fun = lhs::randomLHS)
  
  control <- makeMBOControl() %>%
    setMBOControlTermination(., iters = opt_steps)
  
  run <- mbo(fun = of,
             design = des,
             learner = makeLearner("regr.km", predict.type = "se", covtype = "matern3_2", control = list(trace = FALSE)),
             control = control, 
             show.info = TRUE)
  
  opt_plot <- run$opt.path$env$path %>%
    mutate(Round = row_number()) %>%
    mutate(type = case_when(Round <= n_design ~ "Design",
                            TRUE ~ "mlrMBO optimization")) %>%
    ggplot(aes(x= Round, y= y, color= type)) + 
    geom_point() +
    labs(title = "mlrMBO optimization") +
    ylab("rmse")
  
  print(run$x)
  
  return(list(run = run, plot = opt_plot))
}
des <- generateDesign(n=10,
                      par.set = getParamSet(obj.fun),
                      fun = lhs::randomLHS)
kable(des, format = "html", digits = 4) %>% 
  kable_styling(font_size = 10) %>%
  kable_material_dark()
scatterplot3d(des$eta, des$gamma, des$min_child_weight,
              type = "h", color = "blue", pch = 16)

runs <- do_bayes(n_design = 10, of = obj.fun, opt_steps = 10, seed = 42069)
saveRDS(runs, file = '~/Desktop/Content Strategy - Git Clones/content_strategy/brayden_ross/author_comp_modeling/xgb_24_runs_tuned.RDS')
plot(runs$run)
best_params <- runs$run$x
best_params$booster <- 'gbtree'
best_params$objective <- 'reg:squarederror'
optimal.cv <- xgb.cv(params = best_params,
                     data = dtrain,
                     nrounds = 6000,
                     nthread = 6,
                     nfold = 5,
                     early_stopping_rounds = 35,
                     verbose = 1,
                     print_every_n = 24)
best_params$nrounds <- optimal.cv$best_ntreelimit
best_params$nthread <- 6
ggplot(data = optimal.cv$evaluation_log) + geom_line(aes(x = iter, y = train_rmse_mean),  color = 'red') + 
  geom_line(aes(x = iter, y = test_rmse_mean), color = 'green') + 
  geom_vline(xintercept = optimal.cv$evaluation_log[which.min(optimal.cv$evaluation_log$test_rmse_mean),]$iter, linetype = 'dashed', alpha = 0.5) +
  labs(title = 'RMSE Evaluation Log', 
       subtitle = paste0('Best Iteration: ', optimal.cv$evaluation_log[which.min(optimal.cv$evaluation_log$test_rmse_mean),]$iter))


xgb_model <- xgboost(params = best_params[-10], data = dtrain, nrounds = best_params$nrounds, verbose = 1, print_every_n = 100)
saveRDS(xgb_model, '~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/xgb_mbo_tuned.RDS')
saveRDS(best_params, '~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/xgb_mbo_24_params.RDS')
####.####

####Old Code####
#Max Depth = 4 ETA = 0.3
#Need to implement regularization or gamma metric to avoid overfitting and reduce model complexity
#xgb_model <- xgb.train(data = dtrain, nrounds = 3500, objective = "reg:squarederror", nfold = 10, eval_metric = 'mae', eval_metric = 'rmse',
#                       eta = best_params$eta, early_stopping_rounds = 30,  print_every_n = 10, watchlist = list(train=dtrain, val=dtest), 
#                       max_depth = best_params$max_depth)

#ggplot(data = xgb_model$evaluation_log) + geom_line(aes(x = iter, y = val_rmse),  color = 'red') + geom_line(aes(x = iter, y = train_rmse), color = 'green') + 
#  geom_vline(xintercept = xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_rmse),]$iter, linetype = 'dashed', alpha = 0.5) +
#  labs(title = 'MAE Evaluation Log', 
#       subtitle = paste0('Best Iteration: ', xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_rmse),]$iter))

#xgb_model_final <- xgboost(data = dtrain, max_depth = best_params$max_depth, eta = best_params$eta, nfold = 10,
#                     nrounds = xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_rmse),]$iter, print_every_n = 5)
#grid_save <- expand.grid(nrounds = c(xgb_model$evaluation_log[which.min(xgb_model$evaluation_log$val_rmse),]$iter),
#                         max_depth = c(best_params$max_depth, 11), eta = c(best_params$eta),  subsample = c(1), colsample_bytree = c(1), 
#                         min_child_weight = c(1), gamma = c(0))
#saveRDS(grid_save, file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"video/video_comp_model/xgb_grid.RDS"))
#saveRDS(xgb_model_final,file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),"video/video_comp_model/all_tags_xgboost_model.RDS"))
#pushover.r('XGB Model Finished Running!') 

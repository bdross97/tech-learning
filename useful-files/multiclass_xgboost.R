library(caret)
library(data.table)
library(xgboost)
library(mlr3)
library(ggplot2)
library(kableExtra)
library(scatterplot3d)
library(pROC)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
model_dataset <- read.csv( '~/SageMaker/efs/content_strategy/brayden_ross/new_tax_model_dataset.csv')
quantile(model_dataset$view_time_perc, probs = seq(0, 1, 0.025))
#Remove outliers from view time predictions (bottom and top 5th percentile)
final_model <- model_dataset %>%
  filter(view_time_perc != 0, view_time_perc < 0.001328806110, view_time_perc > 0.000001875453,
         course_age <= 24)
set.seed(42069)
course_list <- final_model %>%
  distinct(course_name, published_date) %>%
  arrange(published_date)
#Train-test split
train_size <- floor(0.7 * nrow(course_list))
train_num <- 1:train_size
train_data <- course_list[train_num, ]
test_data <- course_list[-train_num, ]
#Preparation of Training Data
cl_tags <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/cl_tags_agg.csv')
domain_tags <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/domain_tags.csv')
train_data <- train_data %>% left_join(domain_tags, by = 'course_name')
train_data <- train_data %>% left_join(cl_tags, by = 'course_name')
train_data <- train_data %>%
  mutate(owning.domain = as.character(owning.domain))
train_data$owning.domain[is.na(train_data$owning.domain)] <- 'other'
clusters <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/clustering_courses.csv')
train_data <- train_data %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster),
         owning.domain = as.factor(owning.domain)) %>%
  na.omit()
#Preparation of test data
set.seed(42069)
test_data <- test_data %>% left_join(domain_tags, by = 'course_name')
test_data <- test_data %>%
  mutate(owning.domain = as.character(owning.domain))
test_data$owning.domain[is.na(test_data$owning.domain)] <- 'other'
test_data <- test_data %>% left_join(cl_tags, by = 'course_name')
test_data <- test_data %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster),
         owning.domain = as.factor(owning.domain)) %>%
  na.omit()


#For Loop stuff
n_class_try <- seq(4, 10, 1)
tracking_df <- data.frame(matrix(ncol=10,nrow=0, dimnames=list(NULL, c("iter", "train_mlogloss_mean", "train_mlogloss_std",  "test_mlogloss_mean",  "test_mlogloss_std", "groups", "accuracy", "auc", "max_auc", "min_auc"))))
conf_matrix_list <- list()
for(i in 1:length(n_class_try)) {
  #Aggregate data based on total view time and generate groups in quantiles 1:n 
  model_dataset_agg <- model_dataset %>%
    group_by(course_name) %>%
    summarise(total_vt = sum(view_time_perc)) %>%
    mutate(quants = cut(total_vt, quantile(total_vt, probs = seq(0, 1, 1/n_class_try[i])), include.lowest=TRUE,
                        labels = 1:n_class_try[i] - 1)) %>%
    dplyr::select(course_name, quants) %>%
    ungroup() %>%
    distinct()
  #Join newly created quantile classifications with training data
  train_data_qt <- train_data %>%
    left_join(model_dataset_agg, by = 'course_name')
  train_targets <- as.numeric(train_data_qt$quants) - 1
  tr_matrix <- model.matrix(~.+0,data = train_data_qt[,-c(1:2, 133)]) 
  dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)
 
  #Set classes, other parameters
  numberOfClasses <- length(unique(model_dataset_agg$quants))
  xgb_params <- list("objective" = "multi:softprob",
                     "eval_metric" = "mlogloss",
                     "num_class" = numberOfClasses)
  nround    <- 50
  cv.nfold  <- 7
  print(paste0('Testing ', n_class_try[i], ' Quantiles'))
  # Fit cv.nfold * cv.nround XGB models and save OOF predictions
  cv_model <- xgb.cv(params = xgb_params,
                     data = dtrain, 
                     nrounds = nround,
                     nfold = cv.nfold,
                     verbose = TRUE,
                     prediction = TRUE, 
                     print_every_n = 10)
  # Gather predictive values from cv_model
  OOF_prediction <- data.frame(cv_model$pred) %>%
    mutate(max_prob = max.col(., ties.method = "last"),
           label = train_targets + 1)
  # Generate confusion matrix
  c_mat <- confusionMatrix(factor(OOF_prediction$max_prob),
                  factor(OOF_prediction$label),
                  mode = 'everything')
  #Append 
  conf_matrix_list[[i]] <- c_mat
  #Save best iterations
  best_iter <- cv_model$evaluation_log[which.min(cv_model$evaluation_log$test_mlogloss_mean),]
  roc.multi <- multiclass.roc(train_targets + 1, OOF_prediction$max_prob, quiet = TRUE)
  auc <- auc(roc.multi)
  #Track high and low AUC metrics to determine performance on high and low performer misclassification
  rs <- roc.multi[['rocs']]
  max_auc <- auc(rs[[max(length(rs))]])
  min_auc <- auc(rs[[1]])
  #Gather data into tracking dataframe
  tracking_df <- rbind(tracking_df, data.frame(best_iter, 'groups' = n_class_try[i], 'accuracy' = c_mat$overall[[1]], 'total_auc' = auc, 'max_auc' = max_auc, 'min_auc' = min_auc))
  
}

tracking_df %>%
  ggplot() + geom_line(aes(x = groups, y = test_mlogloss_mean))

tracking_df %>%
  ggplot() + geom_line(aes(x = groups, y = max_auc), color = 'orange') + geom_line(aes(x = groups, y = min_auc), color = 'purple') + theme_bw()

tracking_df %>%
  ggplot() + geom_line(aes(x = groups, y = total_auc), color = 'purple') + theme_bw()

conf_matrix_list[as.numeric(row.names(tracking_df[which.min(tracking_df$test_mlogloss_mean),]))]


#Training Final Model

best_n_groups <- tracking_df[which.min(tracking_df$test_mlogloss_mean),]$groups

model_dataset_agg <- model_dataset %>%
  group_by(course_name) %>%
  summarise(total_vt = sum(view_time_perc)) %>%
  mutate(quants = cut(total_vt, quantile(total_vt, probs = seq(0, 1, 1/best_n_groups)), include.lowest=TRUE,
                      labels = 1:best_n_groups - 1)) %>%
  dplyr::select(course_name, quants) %>%
  ungroup() %>%
  distinct()
#Join newly created quantile classifications with training data
train_data_qt <- train_data %>%
  left_join(model_dataset_agg, by = 'course_name')
train_targets <- as.numeric(train_data_qt$quants) - 1
tr_matrix <- model.matrix(~.+0,data = train_data_qt[,-c(1:2, 133)]) 
dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)

#Gather test data into quantile information for accuracy measurements
test_data_qt <- test_data %>%
  left_join(model_dataset_agg, by = 'course_name')
test_targets <- as.numeric(test_data_qt$quants) - 1
test_matrix <- model.matrix(~.+0,data = test_data_qt[,-c(1:2, 133)])
dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)

numberOfClasses <- length(unique(model_dataset_agg$quants))
xgb_params <- list("objective" = "multi:softprob",
                   "eval_metric" = "mlogloss",
                   "num_class" = numberOfClasses)
nround    <- 50
cv.nfold  <- 7


bst_model <- xgb.train(params = xgb_params,
                       data = dtrain,
                       nrounds = nround, watchlist = list(train=dtrain, val=dtest),
                       print_every_n = 10)

#Testing Final Model
missing_features <- bst_model$feature_names[!bst_model$feature_names %in% colnames(test_matrix)]
mat_add <- matrix(0, nrow = nrow(test_matrix), ncol = length(missing_features))
colnames(mat_add) <- missing_features
test_matrix <- cbind(test_matrix,mat_add)
col.order <- bst_model$feature_names
test_matrix <- test_matrix[,col.order]
test_pred <- predict(bst_model, newdata = test_matrix)
test_prediction <- matrix(test_pred, nrow = numberOfClasses,
                          ncol=length(test_pred)/numberOfClasses) %>%
  t() %>%
  data.frame() %>%
  mutate(label = test_targets + 1,
         max_prob = max.col(., "last"))
# confusion matrix of test set
confusionMatrix(factor(test_prediction$max_prob),
                factor(test_prediction$label),
                mode = "everything")

plot.roc(multiclass.roc(test_targets, test_prediction$max_prob)[["rocs"]][[3]])


m_vars <- c('X1', 'X2' ,'X3' ,'X4' ,'X5')
melted_probs <- melt(test_prediction, measure.vars = m_vars)


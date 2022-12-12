library(caret)
library(data.table)
library(xgboost)
library(mlr3)
library(ggplot2)
library(kableExtra)
library(scatterplot3d)
library(pROC)
library(MASS)
source(file.path(Sys.getenv('AUTH_COMP_REPO_PATH'),'utils/utilities.R'))
model_dataset <- read.csv( '~/SageMaker/efs/content_strategy/brayden_ross/new_tax_model_dataset.csv')
cl_tags <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/cl_tags_agg.csv')
domain_tags <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/domain_tags.csv')
clusters <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/clustering_courses.csv')

quantile(model_dataset$view_time_perc, probs = seq(0, 1, 0.025))
#Remove outliers from view time predictions (bottom and top 5th percentile)
final_model <- model_dataset %>%
  filter(view_time_perc != 0, view_time_perc < 0.001328806110, view_time_perc > 0.000001875453,
         course_age <= 24)

final_model <- final_model %>% left_join(domain_tags, by = 'course_name')
final_model <- final_model %>% left_join(cl_tags, by = 'course_name')

final_model <- final_model %>%
  mutate(owning.domain = as.character(owning.domain))
final_model$owning.domain[is.na(final_model$owning.domain)] <- 'other'
final_model <- final_model %>%
  left_join(clusters, by = 'course_name') %>%
  mutate(cluster = as.factor(cluster),
         owning.domain = as.factor(owning.domain)) %>%
  na.omit()

final_model <- final_model[,-c(2,4,7:13)]

best_n_groups <- 4

final_model_agg <- final_model %>%
  group_by(course_name) %>%
  summarise(total_vt = sum(view_time_perc)) %>%
  mutate(quants = cut(total_vt, quantile(total_vt, probs = seq(0, 1, 1/best_n_groups)), include.lowest=TRUE,
                      labels = 1:best_n_groups)) %>%
  dplyr::select(course_name, quants) %>%
  ungroup() %>%
  distinct()


final_model <- final_model %>% 
  dplyr::select(-view_time_perc, -average_position) %>%
  left_join(final_model_agg, by = 'course_name') %>%
  distinct()

model_matrix <- model.matrix(~.+0,data = final_model[,-c(1, 133)])
model_matrix <- apply(model_matrix, MARGIN = 2, FUN = as.numeric)
tmp <- cor(model_matrix)
tmp[!lower.tri(tmp)] <- 0
model_matrix_new <- as.data.frame(model_matrix[, !apply(tmp, 2, function(x) any(abs(x) > 0.99, na.rm = TRUE))])

model_matrix_new <- cbind(model_matrix_new, final_model[,c(1,133)])
set.seed(42069)
#Train-test split
train_size <- 0.7 * nrow(model_matrix_new)
train_ind <- sample(seq_len(nrow(model_matrix_new)), size = train_size)

train_data <- model_matrix_new[train_num, ]
test_data <- model_matrix_new[-train_num, ]


tmp <- cor(train_data[,c(1:186)])
tmp[!lower.tri(tmp)] <- 0
new_train <- as.data.frame(train_data[, !apply(tmp, 2, function(x) any(abs(x) > 0.99, na.rm = TRUE))])
tmp <- cor(test_data[,c(1:186)])
tmp[!lower.tri(tmp)] <- 0
new_test <- as.data.frame(test_data[, !apply(tmp, 2, function(x) any(abs(x) > 0.99, na.rm = TRUE))])

new_train <- new_train[,colnames(new_train) %in% colnames(new_test)]
new_test <- new_test[,colnames(new_test) %in% colnames(new_train)]


####Ordinal Classification####

new_train$quants <- factor(new_train$quants, levels=1:4, ordered=T)
  
polr_test <- polr(quants ~., data = new_train)
summary(polr_test)
ctable <- coef(summary(polr_test))
p <- round(pnorm(abs(ctable[, "t value"]), lower.tail = FALSE) * 2, 6)
ctable <- cbind(ctable, "p value" = p)

preds <- predict(polr_test, test_matrix)

####OVA Classification####
tracking_df <- data.frame(matrix(ncol=0,nrow=nrow(test_data), dimnames=list(NULL)))


for(i in 1:length(unique(model_dataset_agg$quants))) {
  model_dataset_agg$bin <- ifelse(model_dataset_agg$quants == (i - 1), 1, 0)
  #Join newly created quantile classifications with training data
  train_data_qt <- train_data %>%
    left_join(model_dataset_agg, by = 'course_name')
  train_targets <- as.numeric(train_data_qt$bin)
  tr_matrix <- model.matrix(~.+0,data = train_data_qt[,-c(1:2, 133, 134)]) 
  dtrain <- xgb.DMatrix(data = tr_matrix, label = train_targets)
  
  #Gather test data into quantile information for accuracy measurements
  test_data_qt <- test_data %>%
    left_join(model_dataset_agg, by = 'course_name')
  test_targets <- as.numeric(test_data_qt$bin)
  test_matrix <- model.matrix(~.+0,data = test_data_qt[,-c(1:2, 133, 134)])
  dtest <- xgb.DMatrix(data = test_matrix, label = test_targets)
  
  xgb_params <- list("objective" = "binary:logistic",
                     "eval_metric" = "error")
  nround    <- 50
  cv.nfold  <- 7
  
  
  bst_model <- xgb.train(params = xgb_params,
                         data = dtrain,
                         nrounds = nround, watchlist = list(train=dtrain, val=dtest),
                         print_every_n = 10)
  missing_features <- bst_model$feature_names[!bst_model$feature_names %in% colnames(test_matrix)]
  mat_add <- matrix(0, nrow = nrow(test_matrix), ncol = length(missing_features))
  colnames(mat_add) <- missing_features
  test_matrix <- cbind(test_matrix,mat_add)
  col.order <- bst_model$feature_names
  test_matrix <- test_matrix[,col.order]
  test_pred <- data.frame(g_name = predict(bst_model, newdata = test_matrix, type = 'response'))
  colnames(test_pred) <- paste0('group_', i)
  tracking_df <- cbind(tracking_df, test_pred)
 
}

tracking_df$label <- as.numeric(test_data_qt$quants)
tracking_df$pred <- max.col(tracking_df[,1:4], 'last')

confusionMatrix(factor(tracking_df$pred),
                factor(tracking_df$label),
                mode = "everything")$

ggplot(data = tracking_df, aes(x = ))

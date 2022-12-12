xgb_model <- readRDS('~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/xgb_mbo_tweedie.RDS')
xgb_24 <- readRDS('~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/xgb_mbo_24_months.RDS')
old_test_data <- test_data %>%
  group_by(course_name) %>%
  mutate(course_age = as.numeric(seq(1,n(), 1)))
old_test_data <- old_test_data[,-c(140:163)]
test_targets <- old_test_data$view_time_perc
old_test_data <- as.matrix(old_test_data)
missing_features <- xgb_model$feature_names[!xgb_model$feature_names %in% colnames(old_test_data)]
mat_add <- matrix(0, nrow = nrow(old_test_data), ncol = length(missing_features))
colnames(mat_add) <- missing_features
old_test_data <- cbind(old_test_data,mat_add)
col.order <- xgb_model$feature_names
xgb_all_test_matrix <- old_test_data[,col.order]
xgb_all_test_matrix <- apply(xgb_all_test_matrix, MARGIN = 2, FUN = as.numeric)
dtest <- xgb.DMatrix(data = xgb_all_test_matrix, label = test_targets)
test_data$tw_preds <- predict(xgb_model, dtest)

missing_features <- xgb_24$feature_names[!xgb_24$feature_names %in% colnames(test_matrix)]
mat_add <- matrix(0, nrow = nrow(test_matrix), ncol = length(missing_features))
colnames(mat_add) <- missing_features
test_matrix <- cbind(test_matrix,mat_add)
col.order <- xgb_24$feature_names
xgb_all_test_matrix <- test_matrix[,col.order]
dtest <- xgb.DMatrix(data = xgb_all_test_matrix, label = test_targets)


test_data$twoyr_preds <- exp(predict(xgb_24,dtest))
test_preds <- read.csv('~/Desktop/Content Strategy - Git Clones/author_compensation/video/video_comp_model/Model Training/preds_test.csv')
test_preds <- test_preds %>%
  filter(course_age <= 24)
test_data <- test_data %>%
  group_by(course_name) %>%
  mutate(course_age = seq(1, n(), 1)) %>%
  ungroup() %>%
  left_join(test_preds, by = c('course_name', 'published_date', 'course_age'))

test_data %>%
  ggplot() +
  geom_point(aes(x = twoyr_preds, y = view_time_perc), color = 'pink', alpha = 0.5) + geom_abline(aes(slope = 1, intercept = 0), color = 'green')

test_data %>%
  ggplot() + geom_point(aes(x = twoyr_preds, y = view_time_perc)) + geom_abline(aes(slope = 1, intercept = 0), color = 'red')


MAE(test_data$view_time_perc, pred = test_data$tw_preds)
MAE(test_data$view_time_perc, pred = test_data$twoyr_preds)
MAE(test_data$view_time_perc, pred = test_data$current, na.rm = TRUE)


RMSE(test_data$view_time_perc, pred = test_data$tw_preds)
RMSE(test_data$view_time_perc, pred = test_data$twoyr_preds)
RMSE(test_data$view_time_perc, pred = test_data$current, na.rm = TRUE)

msr_vars <- c('tw_preds', 'twoyr_preds')
melted_df <- melt(test_data[,c(1:3, 6, 216:217, 218)], measure.vars = msr_vars)

melted_df$resids <- melted_df$value - melted_df$view_time_perc

melted_df %>%
  ggplot() + geom_point(aes(x = course_age, y = resids, color = variable)) + facet_wrap(~variable)

avg_resids <- melted_df %>%
  group_by(variable) %>%
  summarise(avg_resid = mean(resids, na.rm = TRUE))
avg_resids

melted_df %>%
  filter(variable != 'current') %>%
  ggplot() + geom_point(aes(x = course_age, y = resids, color = variable, group = variable)) + facet_wrap(~variable) +
  geom_smooth(aes(x = course_age, y = resids), stat = 'smooth', color = 'black', linetype = 'dashed')

totals <- melted_df %>%
  group_by(course_name, variable) %>%
  summarise(total_vt = sum(view_time_perc, na.rm = TRUE),
            total_pred = sum(value, na.rm = TRUE),
            total_resid = sum(resids, na.rm = TRUE)) %>%
  ungroup() %>%
  arrange(desc(total_vt))

quants <- totals %>%
  select(course_name, total_vt) %>%
  distinct() %>%
  mutate(quants = cut(total_vt, quantile(total_vt, probs = seq(0, 1, 0.2)), include.lowest=TRUE,
                               labels = paste("Group", 1:5)))

totals <- totals %>%
  left_join(quants, by = c('course_name', 'total_vt'))

totals %>%
  filter(total_resid != 0) %>%
  ggplot(aes(x = total_vt, y = total_resid, color = variable, group = variable)) + geom_line()

totals %>%
  filter(total_resid != 0, variable != 'current') %>%
  ggplot(aes(x = total_vt, y = total_resid, color = variable, group = variable)) + geom_point() + geom_line() + facet_wrap(~variable)

totals %>%
  ggplot(aes(x = total_vt, y = total_pred, color = variable, group = variable)) + geom_point() + facet_wrap(~quants) + geom_abline(slope = 1,
                                                                                                                                   intercept = 0)

msr_vars <- c('tw_preds', 'twoyr_preds')
melted_dates <- melt(test_data[,c(1:3, 6, 216:217, 218)], measure.vars = msr_vars)

melted_dates$resids <- melted_dates$value - melted_dates$view_time_perc

agg_dates <- melted_dates %>%
  group_by(course_age, variable) %>%
  summarise(avg_residual = mean(resids, na.rm = TRUE),
            sd_residual = sd(resids, na.rm = TRUE),
            rmse_mtm = RMSE(pred = value, view_time_perc, na.rm = TRUE),
            mae_mtm = MAE(pred = value, view_time_perc, na.rm = TRUE))

agg_dates %>%
  ggplot(aes(x = course_age, y = avg_residual, color = variable, group = variable)) + geom_line() + 
  scale_x_continuous(breaks = scales::pretty_breaks(n = 24)) + theme_bw() + geom_hline(yintercept = 0) + geom_point()

agg_dates %>%
  ggplot(aes(x = course_age, y = sd_residual, color = variable, group = variable)) + geom_line() + 
  scale_x_continuous(breaks = scales::pretty_breaks(n = 24)) + theme_bw() + geom_hline(yintercept = 0) + geom_point()

agg_dates %>%
  ggplot(aes(x = course_age, y = rmse_mtm, color = variable, group = variable)) + geom_line() + 
  geom_line(aes(x = course_age, y = mae_mtm), linetype = 'dashed') +  
  scale_x_continuous(breaks = scales::pretty_breaks(n = 24)) + theme_bw() + geom_vline(xintercept = 24) + geom_point()


####Two Year Subsets####
two_yr <- test_data %>%
  filter(course_age <= 24)

two_yr <- two_yr %>%
  left_join(quants, by = c('course_name'))

ggplot(data = two_yr) +
  geom_point(aes(x = tw_preds, y = view_time_perc)) + geom_abline(slope = 1, intercept = 0) + facet_wrap(~quants)

MAE(two_yr$view_time_perc, pred = two_yr$xgb_all_preds)
abs((MAE(two_yr$view_time_perc, pred = two_yr$tw_preds) - 
    MAE(two_yr$view_time_perc, pred = two_yr$current, na.rm = TRUE)) / MAE(two_yr$view_time_perc, pred = two_yr$current, na.rm = TRUE)) * 100
MAE(two_yr$view_time_perc, pred = two_yr$current, na.rm = TRUE)


MAE(test_data$view_time_perc, pred = test_data$xgb_all_preds)
abs(MAE(two_yr$view_time_perc, pred = two_yr$twoyr_preds) - 
      MAE(test_data$view_time_perc, pred = test_data$tw_preds)) / MAE(test_data$view_time_perc, pred = test_data$tw_preds) * 100
MAE(test_data$view_time_perc, pred = test_data$current, na.rm = TRUE)



RMSE(two_yr$view_time_perc, pred = two_yr$xgb_all_preds)
RMSE(two_yr$view_time_perc, pred = two_yr$tw_preds)
RMSE(two_yr$view_time_perc, pred = two_yr$current, na.rm = TRUE)

msr_vars <- c('xgb_all_preds', 'current', 'tw_preds')
melted_df <- melt(two_yr[,c(1:4, 145:147)], measure.vars = msr_vars)

melted_df$resids <- melted_df$value - melted_df$view_time_perc

melted_df %>%
  ggplot() + geom_point(aes(x = course_age, y = resids, color = variable)) + facet_wrap(~variable)

avg_resids <- melted_df %>%
  group_by(variable) %>%
  summarise(avg_resid = mean(resids, na.rm = TRUE))
avg_resids

melted_df %>%
  filter(variable != 'current') %>%
  ggplot() + geom_point(aes(x = course_age, y = resids, color = variable, group = variable)) + facet_wrap(~variable) +
  geom_hline(yintercept = avg_resids$avg_resid[1], color = 'blue') + geom_hline(yintercept = avg_resids$avg_resid[3], color = 'green')

totals <- melted_df %>%
  group_by(course_name, variable) %>%
  summarise(total_vt = sum(view_time_perc, na.rm = TRUE),
            total_pred = sum(value, na.rm = TRUE),
            total_resid = sum(resids, na.rm = TRUE)) %>%
  ungroup() %>%
  arrange(desc(total_vt))

quants <- totals %>%
  select(course_name, total_vt) %>%
  distinct() %>%
  mutate(quants = cut(total_vt, quantile(total_vt, probs = seq(0, 1, 0.2)), include.lowest=TRUE,
                      labels = paste("Group", 1:5)))

totals <- totals %>%
  left_join(quants, by = c('course_name', 'total_vt'))

totals %>%
  filter(total_resid != 0) %>%
  ggplot(aes(x = total_vt, y = total_resid, color = variable, group = variable)) + geom_line()

totals %>%
  filter(total_resid != 0, variable != 'current') %>%
  ggplot(aes(x = total_vt, y = total_resid, color = variable, group = variable)) + geom_line()

totals %>%
  ggplot(aes(x = total_vt, y = total_pred, color = variable, group = variable)) + geom_point() + facet_wrap(~quants) + geom_abline(slope = 1,
                                                                                                                                   intercept = 0)

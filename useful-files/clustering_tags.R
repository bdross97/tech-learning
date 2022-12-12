library(dplyr)
library(cluster)
library(Rtsne)
library(ggplot2)
library(fastDummies)
library(plotly)
course_library_with_tags <- snowflake_query("SELECT DISTINCT a.course_id
, a.course_name
, a.duration_in_hours
, a.is_course_retired
, a.retired_date
 ,lvl1.preflabel as level_1
 ,lvl2.preflabel as level_2
 ,lvl3.preflabel as level_3
FROM analytics.uncertified.px_course_library_with_tags a
LEFT JOIN DVS.CURRENT_STATE.SKILLS_TAGASSIGNMENTS_V2_ASSIGNMENTS b ON a.course_id = b.contentid
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl1 on b.level1 = lvl1.id
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl2 on b.level2 = lvl2.id
left join dvs.current_state.SKILLS_VTO_V2_CONCEPTS lvl3 on b.level3 = lvl3.id
WHERE a.PUBLISHED_DATE IS NOT NULL AND level_1 IS NOT NULL")


cl_tags <- course_library_with_tags %>%
  select(-level_3, -is_course_retired, -retired_date, -duration_in_hours) %>%
  arrange(course_name) %>%
  mutate(level_1 = tolower(level_1),
         level_2 = tolower(level_2))

cl_tags <- dummy_cols(cl_tags, select_columns = c('level_1', 'level_2'))

cl_tags <- distinct(cl_tags, .keep_all = TRUE) %>%
  select(-level_1, -level_2, -course_id)

cl_tags <- as.data.table(cl_tags)[, lapply(.SD, sum), by = .(course_name)]
cl_tags <- as.data.frame(cl_tags)
cl_tags <- cl_tags %>% 
  mutate(across(2:ncol(cl_tags), ~ifelse(.x >= 2, 1, .x)))

write.csv(cl_tags, '~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/cl_tags_agg.csv', row.names = FALSE)

model_data <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_model_dataset.csv')

km_model_data <- model_data %>%
  na.omit()

course_only_data <- km_model_data %>%
  group_by(course_name, published_date) %>%
  summarise(view_time_perc = log(sum(view_time_perc)))

course_only_data <- course_only_data %>%
  left_join(cl_tags, by = 'course_name')

course_only_data[is.na(course_only_data)] <- 0
#Scaling numeric features
# course_only_data[,c(3:4)] <- scale(course_only_data[,c(3:4)])
gower_dist <- daisy(course_only_data[,-c(1:2)],
                    metric = "gower")

summary(gower_dist)

gower_mat <- as.matrix(gower_dist)
#Most similar pair
course_only_data[which(gower_mat == min(gower_mat[gower_mat != min(gower_mat)]),
                       arr.ind = TRUE)[1, ], ]
#Most dissimilar pair
course_only_data[which(gower_mat == max(gower_mat[gower_mat != max(gower_mat)]),
                       arr.ind = TRUE)[1, ], ]


# Calculate silhouette width for many k using PAM

sil_width <- c(NA)

for(i in 41:46){
  
  pam_fit <- pam(gower_dist,
                 diss = TRUE,
                 k = i)
  
  sil_width[i] <- pam_fit$silinfo$avg.width
  print(i)
  
}

# Plot sihlouette width (higher is better)

plot(18:46, sil_width[18:46],
     xlab = "Number of clusters",
     ylab = "Silhouette Width")
lines(18:46, sil_width[18:46])


pam_fit <- pam(gower_dist, diss = TRUE, k = 45)
course_only_data$cluster <- as.factor(pam_fit$clustering)
pam_results <- course_only_data %>%
  ungroup() %>%
  dplyr::select(-course_name, -published_date) %>%
  group_by(cluster) %>%
  do(the_summary = summary(.))

pam_results$the_summary

medoids <- course_only_data[pam_fit$medoids, ]
write.csv(medoids, '~/SageMaker/efs/author_compensation/video/video_comp_model/Production Model Training/cluster_medoids.csv', row.names = FALSE)
tsne_obj <- Rtsne(gower_dist, is_distance = TRUE)

tsne_data <- tsne_obj$Y %>%
  data.frame() %>%
  setNames(c("X", "Y")) %>%
  mutate(cluster = factor(pam_fit$clustering),
         course_name = course_only_data$course_name)

ggplot(aes(x = X, y = Y), data = tsne_data) +
  geom_point(aes(color = cluster)) + theme_bw()
tsne_data <- tsne_data %>%
  left_join(course_only_data[,c(1,3)], by = 'course_name')
plot_ly(x=tsne_data$X, y=tsne_data$Y, z=tsne_data$view_time_perc, type="scatter3d", mode="markers", color=tsne_data$cluster)

write_data <- tsne_data %>%
  select(course_name, cluster)

write.csv(write_data, '~/SageMaker/efs/content_strategy/brayden_ross/author_comp_modeling/clustering_courses.csv', row.names = FALSE)

tsne_data <- tsne_data %>%
  left_join(course_only_data[,c(1,3)], by = 'course_name')

plot_ly(x=tsne_data$X, y=tsne_data$Y, z=log(tsne_data$view_time_perc), type="scatter3d", mode="markers", color=tsne_data$cluster)

model_data <- model_data %>%
  left_join(course_only_data[,c(1,ncol(course_only_data))], by = 'course_name')
model_data %>%
  ggplot(aes(x = course_age, y = log(view_time_perc))) +
  geom_point(aes(color = cluster)) + theme_bw()

plot_ly(x=model_data$course_age, y=log(model_data$view_time_perc), z=model_data$duration_in_hours, type="scatter3d", mode="markers", color=model_data$cluster)

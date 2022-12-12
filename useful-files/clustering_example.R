library(dplyr)
library(cluster)
library(Rtsne)
library(ggplot2)
library(fastDummies)
model_data <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_model_dataset.csv')

km_model_data <- model_data %>%
  na.omit()

course_only_data <- km_model_data %>%
  group_by(course_name, published_date) %>%
  summarise(view_time_perc = sum(view_time_perc),
            average_position = max(unique(average_position)),
            duration_in_hours = unique(duration_in_hours),
            level_1_total_sat = sum(level_1_saturation),
            level_2_total_sat = sum(level_2_saturation))

km_model_dist <- km_model_data %>%
  dplyr::select(course_name, level_1, level_2) %>%
  distinct()

course_only_data <- course_only_data %>%
  full_join(km_model_dist, by = 'course_name') %>%
  distinct(course_name, .keep_all = TRUE)

course_only_data[is.na(course_only_data)] <- 0
#Scaling numeric features
course_only_data[,c(3:7)] <- scale(course_only_data[,c(3:7)])

gower_dist <- daisy(course_only_data[, -c(1:2)],
                    metric = "gower",
                    type = list(logratio = 3))

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

for(i in 18:25){
  
  pam_fit <- pam(gower_dist,
                 diss = TRUE,
                 k = i)
  
  sil_width[i] <- pam_fit$silinfo$avg.width
  print(i)
  
}

# Plot sihlouette width (higher is better)

plot(10:25, sil_width[10:25],
     xlab = "Number of clusters",
     ylab = "Silhouette Width")
lines(10:25, sil_width[10:25])

pam_fit <- pam(gower_dist, diss = TRUE, k = 13)
course_only_data$cluster <- as.factor(pam_fit$clustering)
pam_results <- course_only_data %>%
  ungroup() %>%
  dplyr::select(-course_name, -published_date) %>%
  group_by(cluster) %>%
  do(the_summary = summary(.))

pam_results$the_summary

course_only_data[pam_fit$medoids, ]

tsne_obj <- Rtsne(gower_dist, is_distance = TRUE)

tsne_data <- tsne_obj$Y %>%
  data.frame() %>%
  setNames(c("X", "Y")) %>%
  mutate(cluster = factor(pam_fit$clustering),
         name = course_only_data$course_name)

ggplot(aes(x = X, y = Y), data = tsne_data) +
  geom_point(aes(color = cluster)) + facet_wrap(~cluster) + theme_bw()

ggplot(data = course_only_data, aes(x = level_1_total_sat, y = level_2_total_sat, group = cluster, color = cluster)) + geom_point()


course_clusters <- course_only_data %>%
  dplyr::select(course_name, cluster)
write.csv(course_clusters, '~/SageMaker/efs/content_strategy/brayden_ross/clusters.csv', row.names = FALSE)

library(dplyr)
library(fastDummies)
library(MA)
kmvar <- function(mat, clsize=10, method=c('random','maxd', 'mind', 'elki')){
  k = ceiling(nrow(mat)/clsize)
  #First k-means computation
  km.o = kmeans(mat, k)
  labs = rep(NA, nrow(mat))
  #Cluster assignment step using euclidean distance between center and point
  centd = lapply(1:k, function(kk){
    euc = t(mat)-km.o$centers[kk,]
    sqrt(apply(euc, 2, function(x) sum(x^2)))
  })
  #Assignment step for all data points
  centd = matrix(unlist(centd), ncol=k)
  clsizes = rep(0, k)
  if(method[1]=='random'){
    ptord = sample.int(nrow(mat))
  } else if(method[1]=='elki'){
    ptord = order(apply(centd, 1, min) - apply(centd, 1, max))
  } else if(method[1]=='maxd'){
    ptord = order(-apply(centd, 1, max))
  } else if(method[1]=='mind'){
    ptord = order(apply(centd, 1, min))
  } else {
    stop('unknown method')
  }
  #Assignment for each point relative to cluster size until cluster is filled
  for(ii in ptord){
    bestcl = which.max(centd[ii,])
    labs[ii] = bestcl
    clsizes[bestcl] = clsizes[bestcl] + 1
    if(clsizes[bestcl] >= clsize){
      centd[,bestcl] = NA
    }
  }
  return(labs)
}

model_data <- read.csv('~/SageMaker/efs/content_strategy/brayden_ross/new_tax_model_dataset.csv')

km_model_data <- model_data %>%
  na.omit()

km_model_data <- dummy_cols(km_model_data, select_columns  = c('level_1', 'level_2')) %>%
  select(-level_1, -level_2)

course_only_data <- km_model_data %>%
  group_by(course_name, published_date) %>%
  summarise(view_time_perc = sum(view_time_perc),
            average_position = max(unique(average_position)),
            duration_in_hours = unique(duration_in_hours),
            level_1_total_sat = sum(level_1_saturation),
            level_2_total_sat = sum(level_2_saturation))

km_model_dist <- km_model_data %>%
  select(-published_date, -view_time_perc, -average_position, -duration_in_hours, -platform_size,
         -level_1_saturation, -level_2_saturation, -course_age, -course_age_saturation_lvl1,
         -course_age_saturation_lvl2) %>%
  distinct()

course_only_data <- course_only_data %>%
  left_join(km_model_dist, by = 'course_name')

course_only_data[is.na(course_only_data)] <- 0


test_cov <- cov(course_only_data[,-c(1,2)],use="pairwise.complete.obs")
test_eigen <- eigen(test_cov)
pca_test <- prcomp(x = course_only_data[,-c(1,2)])
course_names <- course_only_data[,1]
PVE <- test_eigen$values / sum(test_eigen$values)
PVEplot <- qplot(c(1:length(PVE)), PVE) + 
  geom_line() + 
  xlab("Principal Component") + 
  ylab("PVE") +
  ggtitle("Scree Plot") +
  ylim(0, 1) + theme(panel.border = element_blank(), panel.grid.major = element_blank(), axis.line = element_line(colour = "black")) + theme_bw()
PVEplot
PVEplot <- qplot(c(1:length(PVE[PVE >= 0.001])), PVE[PVE > 0.001]) + 
  geom_line() + 
  xlab("Principal Component") + 
  ylab("PVE") +
  ggtitle("Scree Plot") +
  ylim(0, 1) + theme(panel.border = element_blank(), panel.grid.major = element_blank(), axis.line = element_line(colour = "black")) + theme_bw()
PVEplot

pca_test <- pca_test$x[,c(1:length(PVE[PVE >= 0.001]))]

stderrtest <- km_model_data %>%
  group_by(course_name) %>%
  summarise(stdevVT = sd(view_time_perc),
            course_avg =  mean(view_time_perc),
            #Compute total standard deviation using view time standard deviation and product average revenue
            stdevtot = stdevVT/course_avg) %>%
  na.omit()
wghtmean <- weighted.mean(stderrtest$stdevtot, stderrtest$course_avg)

sampsize <- function(stdev, marg, critval) { 
  return (((critval * stdev)/(marg))^2)
}

required_sample <- sampsize(wghtmean, .20, 1.28)
#Compute cluster size (number of clusters to initialize) based on confidence level
cluster_size <- round(nrow(pca_test)/ required_sample)
#Gather cluster results using KMvar function from before.
cluster_results <- kmvar(pca_test, clsize = cluster_size)
final <- as.data.frame(pca_test)

ggplot(data = final, aes(x = PC1, y = PC2)) + geom_point()  + 
  # ylim(c(-10, 10)) + xlim(-50, 100) + 
  facet_wrap(~ factor(cluster_results)) +
  theme_bw() + theme(panel.border = element_blank(), panel.grid.major = element_blank(),
                     axis.line = element_line(colour = "black")) + labs(title = 'PCA Clusters') + geom_vline(aes(xintercept = 0))

#Append cluster results for selection among cluster groups
course_only_data$cluster <- cluster_results
clust_model_dat <- course_only_data
# clust_model_dat <- model_data %>%
#   left_join(course_only_data, by = c('course_name', 'published_date'))


#Group by cluster ID and compute median characteristics for all data points
median_chars <- clust_model_dat %>%
  ungroup() %>%
  select(-course_name, -published_date) %>%
  group_by(cluster) %>%
  summarise(across(everything(), median))
#Gather row sums for median characteristics
median_chars$sum <- rowSums(median_chars[,2:length(median_chars)])
#select only row sums for median characteristics to find closest observation in cluster
median_chars <- median_chars[,c(1,ncol(median_chars))]
#Merge back into trans_chars data
clust_model_dat <- merge(clust_model_dat, median_chars, by = 'cluster')
#Gather individual observation row sums
clust_model_dat$sum2 <- rowSums(clust_model_dat[,4:(ncol(clust_model_dat) - 1)])

#
clust_model_dat <- clust_model_dat %>%
  mutate(sum_diff = abs(sum2 - sum))

#get 45% split of observations by cluster based on closest median observations (nearest 45% of observations to median)
treat_courses <- clust_model_dat %>%
  arrange(cluster, sum_diff) %>%
  group_by(cluster) %>% 
  slice(1:round(nrow(clust_model_dat)/max(clust_model_dat$cluster)*0.45)) %>%
  select(course_name, cluster) %>%
  distinct()
#Assign variable for Treatment Cluster
treat_courses$is_treat <- 1
#Merge treatment stores back into wide data frame
new_model_data <- merge(clust_model_dat, treat_courses, all.x = TRUE, by = c('course_name', 'cluster'))
#Assign all other stores that are not treatment stores a 0
new_model_data$is_treat[is.na(new_model_data$is_treat)] <- 0
#Filter out treatment stores for selection of control stores
control_courses <- new_model_data %>%
  filter(is_treat != 1) %>%
  arrange(cluster, sum_diff) %>%
  group_by(cluster) %>%
  select(course_name, cluster) %>%
  distinct()
  
control_courses$is_cont <- 2

new_model_data <- merge(new_model_data, control_courses, all.x = TRUE, by = c('course_name', 'cluster'))
new_model_data$is_cont[is.na(new_model_data$is_cont)] <- 0
new_model_data$treat_control <- rowSums(new_model_data[,c('is_cont','is_treat')])
new_model_data <- new_model_data %>%
  # filter(treat_control != 0) %>%
  select(-is_cont, -is_treat)


final$treat_control <- new_model_data$treat_control

#Create new feature for coloring points on PCA graph for cluster/treatment/control courses
final$treat_control <- as.factor(ifelse(final$treat_control == 1, 'Treatment Store', 'Control Store'))
#Plot PCA graph, this time colored for treatment and control stores. Change transparency of cluster stores not selected for better visual understanding
ggplot(data = final, aes(x = PC1, y = PC2)) + geom_point(aes(color = treat_control, 
                                                             alpha = treat_control == 'Treatment Store')) + 
  scale_alpha_manual(values = c(1, 0.5), guide = "none") +
  xlim(range(final[,1])) + ylim(range(final[,2])) + facet_wrap(~ factor(cluster_results)) + 
  theme_bw() + theme(panel.border = element_blank(), panel.grid.major = element_blank(), 
  axis.line = element_line(colour = "black")) + labs(title = 'PCA Clusters', 
                                                     subtitle = 'Colored for Treatment and Control 
                                                     Stores within Cluster')


int   <- rep(1,nrow(new_model_data))
slope <- rep(1,nrow(new_model_data))
ref   <- data.frame(int, slope)

new_model_data$treat_control <- as.factor(ifelse(new_model_data$treat_control == 1, 'Treatment Course', 'Control Course'))
#This graph shows the fit of the median chara
ggplot(data = new_model_data) + geom_point(aes(x = sum, y = sum2, color = treat_control,
                                                                       alpha = treat_control == 'Treatment Store')) + 
  scale_alpha_manual(values = c(1, 0.5), guide = "none") + geom_abline(data = ref, aes(intercept=int, slope=slope), color="black", alpha = 0.5) +
  xlim(range(new_model_data$sum)) + ylim(range(new_model_data$sum2)) + xlab('Median Characteristic Sums') +
  ylab('Course Characteristic Sums') + 
  theme_bw() + theme(panel.border = element_blank(), panel.grid.major = element_blank(), 
                     axis.line = element_line(colour = "black")) + labs(title = 'Median Characteristics within Clusters', 
                                                                        subtitle = 'Colored for Treatment and Control \nCourses within Cluster')

final_model_data <- new_model_data %>%
  select(course_name, -treat_control, cluster)

final_model_data <- model_data %>%
  left_join(final_model_data, by = 'course_name')

#### Default R Options ####
options(scipen = 999)


#### Library Sourcing ####
require(tidyverse)
require(lubridate)
require(uuid)
require(data.table)
require(reticulate)
require(botor)
#### Legacy seekr dependencies ####
CheckCache <- function(fn.name, args, verbose = FALSE){
  if(!exists("cache.env")){
    cache.env <<- new.env(parent = .GlobalEnv)
  }
  hash <- paste(fn.name, digest::digest(args), sep="_")
  if(verbose) message(Sys.time(), " CheckCache, checking for ", hash)
  if(exists(hash, envir=cache.env)){
    if(verbose) message(Sys.time(), " CheckCache, cache found, returning ", hash)
    return(get(x=hash, envir = cache.env))
  } else {
    message(Sys.time(), " CheckCache, no cache for ", hash)
    return(NULL)
  }
}

SaveCache <- function(obj, fn.name, args, verbose = FALSE){
  if(!exists("cache.env")){
    cache.env <<- new.env(parent = .GlobalEnv)
  }
  hash <- paste(fn.name, digest::digest(args), sep="_")
  assign(x = hash, value = obj, envir = cache.env)
  message(Sys.time(), " SaveCache, saving ", hash)
  return(exists(hash, envir= cache.env))
}

#### Snowflake Connection ####

# Connection
snowflakeConnection <- function (env_var_userid = Sys.getenv("SNOWFLAKE_UID"),
                                 env_var_pwd = Sys.getenv("SNOWFLAKE_PWD"),
                                 db = "ANALYTICS"){
  conn <- odbc::dbConnect(
    odbc::odbc(),
    Driver = Sys.getenv("SNOWFLAKE_DRV"),
    Server = Sys.getenv("SNOWFLAKE_SERVER"),
    UID = env_var_userid,
    PWD = env_var_pwd,
    Database = db
  )
  return(conn)
}

# Query
snowflake_query <- function(query, db = "ANALYTICS", lower_case = TRUE, checkCache = FALSE,
                            env_var_userid = Sys.getenv("SNOWFLAKE_UID"),
                            env_var_pwd = Sys.getenv("SNOWFLAKE_PWD")){
  require(odbc)
  
  if (checkCache) {
    args <- c(as.list(environment()), list())
    cached.result <- CheckCache("snowflake_query", args,
                                verbose = TRUE)
    if (!is.null(cached.result))
      return(cached.result)
  }
  
  sfcon <- snowflakeConnection(db = db, env_var_userid = env_var_userid, env_var_pwd = env_var_pwd)
  on.exit(odbc::dbDisconnect(sfcon))
  sf_results <- odbc::dbGetQuery(sfcon, query)
  
  if(lower_case){
    sf_results <- dplyr::rename_all(sf_results, tolower)
  }
  SaveCache(sf_results, fn.name = "snowflake_query", args = args)
  
  return(sf_results)
}

# Write
letItSnow <- function(data, schema = "SANDBOX", tableName, database = "ANALYTICS", overwrite = FALSE, append = FALSE,
                      env_var_userid = Sys.getenv("SNOWFLAKE_UID"),
                      env_var_pwd = Sys.getenv("SNOWFLAKE_PWD")){
  require(odbc)
  require(DBI)
  require(stringr)
  sfcon <- snowflakeConnection(db = database, env_var_userid = env_var_userid, env_var_pwd = env_var_pwd)
  on.exit(odbc::dbDisconnect(sfcon))
  
  # rename columns to uppercase
  if(any(str_detect(names(data), "[:lower:]"))){
    print("WARNING: names(data) included lower case characters and have been changed toupper")
    data <- dplyr::rename_all(data, toupper)
  }
  
  if(overwrite){
    # drop VARCHAR limit
    dt <- DBI::dbDataType(sfcon, data)
    dt2 <- str_replace_all(dt, "\\(255\\)", "")
    names(dt2) <- names(dt)
    
    tbl_id <- DBI::Id(catalog = database, schema = schema, table = tableName)
    result <- DBI::dbWriteTable(sfcon, tbl_id, data, overwrite = overwrite, field.types = dt2)
  } else {
    tbl_id <- DBI::Id(catalog = database)
    result <- DBI::dbWriteTable(sfcon, schema, tableName, data, append = append)
  }
  return(result)
}

letItSnow2 <- function (data, schema, tableName, database = "ANALYTICS", 
                        snowflake_conn = NULL,
                        overwrite = FALSE, append = TRUE)  {
  require(odbc)
  require(DBI)
  require(stringr)
  
  if (is.null(snowflake_conn)) { 
    sfcon <- snowflakeConnection(db = database)
  } else { 
    sfcon <- snowflake_conn
  } 
  on.exit(odbc::dbDisconnect(sfcon))
  tbl_id <- DBI::Id(catalog = database, schema = schema, table = tableName)
  
  if (any(str_detect(names(data), "[:lower:]"))) {
    print("WARNING: names(data) included lower case characters and have been changed toupper")
    names(data) <- toupper(names(data))
  }
  
  # new table or existing table to overwrite.
  if (!append) { 
    dt <- DBI::dbDataType(sfcon, data)
    dt2 <- str_replace_all(dt, "\\(255\\)", "")
    names(dt2) <- names(dt)
    result <- DBI::dbWriteTable(sfcon, tbl_id, data, overwrite = overwrite, field.types = dt2)
  } 
  # existing table
  else { 
    result <- DBI::dbWriteTable(sfcon, tbl_id, data, append = append)
  }
  return(result)
}

#### Lodestone Connection ####
LodestoneConnection <- function(lodestone_user = Sys.getenv("LODESTONE_UID"),
                                lodestone_pwd = Sys.getenv("LODESTONE_PWD")){
  require(RPostgreSQL)
  drv <- DBI::dbDriver("PostgreSQL")
  return(RPostgreSQL::dbConnect(
    drv, host = "contentstrategy-staging.cy2huujgnr9c.us-west-2.rds.amazonaws.com",
    port="5432",
    dbname="contentstrategy",
    user = lodestone_user,
    password = lodestone_pwd))
}

LodestoneQuery <- function(query){
  require(RPostgreSQL)
  con <- LodestoneConnection()
  on.exit(RPostgreSQL::dbDisconnect(con))
  output <- DBI::dbGetQuery(con, query)
  return(output)
}

WriteLodestone <- function(data, tableName, append = FALSE, overwrite = FALSE, row.names = FALSE){
  require(RPostgreSQL)
  con <- LodestoneConnection()
  on.exit(RPostgreSQL::dbDisconnect(con))
  output <- DBI::dbWriteTable(con, tableName, value = as.data.frame(data), row.names = row.names, overwrite = overwrite, append = append)
  return(output)
}

#### Custom Funcntions ####

# Factor -> numeric function
as.numeric.factor <- function(x) {as.numeric(levels(x))[x]}


#Explonential Decay function
exp_decay <- function(target){
  rate = (target^(1/36)) - 1
  temp_df <- data.frame(month = 1:36)
  temp_df <- temp_df %>%
    dplyr::mutate(total_royalties = 1*(1+rate)^month)
  
  difference = diff(temp_df$total_royalties)
  difference <- c(0,difference)
  
  temp_df$MoM_royalties <- round(difference,2)
  
  final_projections <- temp_df %>%
    dplyr::arrange(desc(MoM_royalties)) %>%
    dplyr::mutate(month = 1:36) %>%
    dplyr::select(-total_royalties)
  
  return(final_projections)
}

#Opportunity Home Variable
stage_opportunity_home <- "https://internal.vnerd.com/content-home/api/v3/opportunity"

#### Ben's Functions ####
# pushover notifications
pushover.r <- function(msg) {
  pushoverr::pushover(message = msg, user = Sys.getenv("PUSH_USR"), app = Sys.getenv("PUSH_PWD"))
}

#use python installation
# python_path <- system('which python', intern = TRUE)
use_python('/usr/bin/python3')
sagemaker <- import('sagemaker')
boto3 <- import('boto3')
io <- import('io')


list_s3_objects <- function() {
  system('aws s3 ls s3://ip-datalake-us-west-2/customer/data-science/sagemaker_storage/brayden-ross/')
}

write_s3_data <- function(data, filename) {
  #Initialize session information
  # session <- sagemaker$Session(boto3$)
  region <- 'us-west-2'
  key <- paste0('customer/data-science/sagemaker_storage/brayden-ross/', filename)
  account <- "520876732921"
  #Begin S3 client authorization
  s3_client <- boto3$client('s3')
  #Write data/object to S3 bucket
  write.csv(data, filename)
  s3_client$put_object(Body = filename, Bucket = 'ip-datalake-us-west-2', Key = key)
  print("Success! S3 File List:") 
  system('aws s3 ls s3://ip-datalake-us-west-2/customer/data-science/sagemaker_storage/brayden-ross/')
  
}

delete_s3_object <- function(){
  system('aws s3 ls s3://ip-datalake-us-west-2/customer/data-science/sagemaker_storage/brayden-ross/')
  object_name <- readline(prompt =  "Which file would you like to delete? ")
  #Initialize session information
  # session <- sagemaker$Session()
  region <- 'us-west-2'
  bucket <- 'ip-datalake-us-west-2'
  del_key <- paste0('customer/data-science/sagemaker_storage/brayden-ross/', object_name)
  account <- "520876732921"
  #Begin S3 client authorization
  s3_client <- boto3$client('s3')
  response <- s3_client$delete_object(Bucket = bucket, Key = del_key)$DeleteMarker
  if(response == TRUE){
    print("Success!")
    system('aws s3 ls s3://ip-datalake-us-west-2/customer/data-science/sagemaker_storage/brayden-ross/')
  } else {
    print('Failed: File was either under a different name or did not exist')
  }
}

read_s3_object <- function(object, read_fun, ...) {
  # session <- sagemaker$Session()
  region <- 'us-west-2'
  bucket <- 'ip-datalake-us-west-2'
  account <- "520876732921"
  read_key <- paste0("s3://", bucket, '/customer/data-science/sagemaker_storage/brayden-ross/', object)
  #Begin S3 client authorization
  s3_read(read_key, read_fun)
}

# obj_path <-  '/home/ec2-user/SageMaker/efs/author_compensation/video/video_comp_model/Production Model Training/cluster_medoids.csv'
# write_s3_object(obj_path, 'medoids.csv')
# delete_s3_object()

write_s3_rds <- function(object, filename) {
  #Initialize session information
  region <- 'us-west-2'
  bucket <- 'ip-datalake-us-west-2'
  account <-"520876732921"
  #Begin S3 client authorization
  s3_client <- boto3$client('s3')
  #Write object to S3 bucket
  write_key <- paste0("s3://", bucket, '/customer/data-science/sagemaker_storage/brayden-ross/', filename)
  s3_write(object, saveRDS, write_key)
  print("Success! S3 File List:") 
  system('aws s3 ls s3://ip-datalake-us-west-2/customer/data-science/sagemaker_storage/brayden-ross/')
  
}

# xgb_24 <- readRDS('~/SageMaker/efs/author_compensation/video/prod_framework/xgb_mbo_24_months.RDS')
# write_s3_rds(object = xgb_24, filename = 'xgb_model.RDS')
# read_s3_object('quant_model.RDS', readRDS)

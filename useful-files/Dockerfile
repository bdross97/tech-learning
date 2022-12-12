#Source tidyverse image, contains tidyverse and devtools - saves on installation time
FROM rocker/tidyverse:latest

#Install all odbc dependencies and unixodbc 

RUN apt-get update && \
    apt-get install -y apt-utils && \
    apt-get install -y libssl-dev libffi-dev && \
#Install python to use reticulate package
    apt-get install -y libpython3-dev && \
    apt-get install -y python3-pip && \
    apt-get install -y vim
RUN apt-get update -qq && apt-get -y --no-install-recommends install \
    apt-transport-https \
    tk8.6-dev \
    cron \
    nano \
    curl \
    gnupg \
    unixodbc \
    && install2.r odbc \
    smooof \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/*

RUN pip install sagemaker

RUN pip install boto3

#AWS CLI dependencies

RUN apt update && apt install -y curl unzip groff

#AWS installation

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
	./aws/install


#install git

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install --assume-yes git

#Install dev odbc dependencies

RUN apt-get install -y iodbc libiodbc2-dev libssl-dev

#Install only required packages

RUN R -e "install.packages(c('lubridate', 'mlr3', 'scatterplot3d', 'pushoverr', 'ParamHelpers', 'DiceKriging', 'kableExtra', 'mlrMBO', 'caret', 'DBI', 'xgboost', 'uuid', 'dummies', 'odbc', 'data.table','dtplyr', 'stringr', 'gower', 'cluster', 'fastDummies', 'reticulate', 'botor'), dep = TRUE)"

#Copy appropriate dataset preparation and training scripts

COPY boosting_algorithm_training.R xgboost_mbo_tuning.R revenue_pool_forecast.R master_script.R production_dataset_build.R utilities.R master_tag_list.R .author_comp_env quantile_class_pred.R /home/scripts/

#Snowflake Setup

COPY deploy_snowflake.sh ./
RUN chmod +x deploy_snowflake.sh

RUN odbc_version=2.23.2 /deploy_snowflake.sh

RUN cat /home/scripts/.author_comp_env >> /usr/local/lib/R/etc/Renviron

#CMD should be in place here to only run when batch job initiates
#Run master tag set list creation first to incorporate new tags into training methods

WORKDIR /home/scripts/

#Single CMD function to run master script containing all training code

CMD ["Rscript", "master_script.R"]

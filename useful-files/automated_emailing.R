# This section has to be loaded and render the HTML output before gmailR is loaded due to a base R conflict
library(rmarkdown)
source('~/author_compensation/video/prod_framework/utilities.R')
setwd('~/content_strategy/brayden_ross')
rmarkdown::render('auth_comp_monthly_report.Rmd')
# Can now load gmailr to avoid base conflict
library(gmailr)
gm_auth_configure(path = '~/content_strategy/brayden_ross/google_auth.json')
# create message with newly generated report
report_message <- gm_mime() %>%
  gm_to('brayden-ross@pluralsight.com') %>%
  gm_subject("Monthly Viewership + Payment Report") %>%
  gm_attach_file("auth_comp_monthly_report.html")
# Send monthly report to myself
gm_send_message(report_message)
1
# Notify me that email sent successfully
pushover.r(msg = 'Monthly Report Successfully Completed and Emailed!')

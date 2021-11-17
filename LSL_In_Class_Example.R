library(sparklyr)
library(tidyverse)

#smaller version of main program that runs quicker
sc <- spark_connect(master = "local")
train_inclass <- spark_read_csv(sc,path = ".../validation_timeseries.csv",memory=FALSE)
test_inclass <- spark_read_csv(sc,path = ".../test_timeseries.csv",memory=FALSE)

train_inclass <- train_inclass %>%
  mutate(date = as.Date(date), 
         date = month(date),
         date = as.character(date)) %>%
  select(-c(fips,score))

train_inclass <- train_inclass %>%
  sample_n(10000)

test_inclass <- test_inclass %>%
  mutate(date = as.Date(date), 
         date = month(date),
         date = as.character(date)) %>%
  select(-c(fips,score))

test_inclass <- test_inclass %>%
  sample_n(10000)

#start here
linreg_inclass <- train_inclass %>%
  ml_linear_regression(PRECTOT~.)

pred_inclass <- ml_predict(linreg_inclass, test_inclass)

#gives r2
ml_regression_evaluator(pred_inclass, label_col="PRECTOT", metric_name="r2")


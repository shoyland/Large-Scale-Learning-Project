#Large Scale Learning Test
#install.packages("sparklyr")
#install.packages("tidyverse")
#install.packages("installr")
library(sparklyr)
library(tidyverse)
#library(installr)
#spark_install(version = "3.1.1")
#install.java(version = 11,
#             page_with_download_url = "http://jdk.java.net/java-se-ri/",
#             path = "C:/java")
sc <- spark_connect(master = "local")
train <- spark_read_csv(sc,path = "~/train_timeseries.csv",memory=FALSE)
train <- train %>%
  mutate(date = as.Date(date), 
         date = month(date),) %>%
  mutate(date = as.character(date)) %>%
  select(-c(fips,score))

test <- spark_read_csv(sc,path = "~/test_timeseries.csv",memory=FALSE)
test <- test %>%
  mutate(date = as.Date(date), 
         date = month(date),) %>%
  mutate(date = as.character(date)) %>%
  select(-c(fips,score))

#visualize data
#install.packages("dbplot")
library(dbplot)
train %>%
  dbplot_histogram(T2MDEW) +
  xlab("Dew Point at 2 Meters (C)")
train %>%
  dbplot_histogram(PS) +
  xlab("Surface Pressure (kPa)")
train %>%
  dbplot_histogram(QV2M) +
  xlab("Humidity at 2 Meaters (g/kg)")
train %>%
  dbplot_histogram(T2M) +
  xlab("Temperature at 2 Meters (C)")


#Machine Learning Algorithms
ptm <- proc.time()
#linear regression
linreg <- train %>%
  ml_linear_regression(PRECTOT~date+PS+QV2M+T2M+T2MDEW+T2MWET +T2M_MAX+T2M_MIN
                       +T2M_RANGE+TS+WS10M+WS10M_MAX+WS10M_MIN+WS10M_RANGE
                       +WS50M+WS50M_MAX+WS50M_MIN+WS50M_RANGE)

pred <- ml_predict(linreg, test)

#gives r2
ml_regression_evaluator(pred,label_col="PRECTOT", metric_name="r2")
#gives mean square error
ml_regression_evaluator(pred, label_col="PRECTOT", metric_name="mse")
proc.time() - ptm

#logistic regression
ptm <- proc.time()
#create new binary response variable
train_lr <- train %>%
  mutate(rain = case_when(
    PRECTOT == 0 ~ 0,
    PRECTOT > 0 ~ 1)) %>%
  select(-PRECTOT)

test_lr<- test %>%
  mutate(rain = case_when(
    PRECTOT == 0 ~ 0,
    PRECTOT > 0 ~ 1)) %>%
  select(-PRECTOT)

logreg <- train_lr %>%
  ml_logistic_regression(rain~date+PS+QV2M+T2M+T2MDEW+T2MWET
                         +T2M_RANGE+TS+WS10M+WS10M_RANGE
                         +WS50M+WS50M_RANGE)


pred <- ml_predict(logreg, test_lr)

#gives Area Under ROC
ml_binary_classification_evaluator(pred)

proc.time() - ptm
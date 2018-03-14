# SparkWithR.R
# from blog: http://blog.godatadriven.com/sparkr-just-got-better.html
.libPaths(c(.libPaths(), '/root/spark/R/lib'))
Sys.setenv(SPARK_HOME = '/root/spark')
Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), '/root/spark/bin', sep=':'))

library(SparkR)
library(magrittr) # Pipes
library(dplyr)
# initialize the Spark Context
sc <- sparkR.init()
sqlContext <- sparkRSQL.init(sc)

bank <- read.csv("/home/renzo/bank.csv",sep = ";") %>% 
  data.frame()

bank2 <- read.csv("https://s3-us-west-2.amazonaws.com/bank.uci/bank-full.csv", 
                  sep = ";") %>% # read S3 file
  data.frame()
# create a data frame using the createDataFrame object
df <- createDataFrame(sqlContext, bank) 
head(df)
str(df)
glimpse(df)

# bancos <- df %>% 
# collect()

model <- glm(age ~ balance + education, data = df,
             family = "gaussian")
summary(model)

predictions <- predict(model, newData = df)

head(predictions)

predictions %>% 
  SparkR::select("age","prediction") %>% 
  head

df %>% head()

df %>% 
  SparkR::filter("balance>7") %>% 
  SparkR::head()

df %>% 
  SparkR::groupBy("education") %>% 
  SparkR::summarize(MinAge=min(df$age)) %>% 
  head

df %>% 
  SparkR::groupBy("education") %>% 
  SparkR::summarize(Count=n()) %>% 
  head

head(SparkR::arrange(SparkR::agg(SparkR::groupBy(df,df$age),
                                 total = sum(df$balance)),
                     df$age))

df %>% 
  SparkR::groupBy(df$age) %>% 
  SparkR::agg(total=sum(df$balance)) %>% 
  SparkR::arrange(df$age) %>% 
  head()



# SparkSQL ----------------------------------------------------------------

registerTempTable(df,"dfBank")

jobs <- SparkR::sql(sqlContext,"SELECT job from dfBank")

jobs %>% 
  head

result2 <- SparkR::sql(sqlContext,
                       "SELECT age,job,balance,loan from dfBank WHERE balance>=1000")

sqlContext %>% 
  SparkR::sql("SELECT age,job,balance,loan from dfBank WHERE balance>=1000") %>% 
  SparkR::collect() %>% 
  count()



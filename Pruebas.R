# Sparklyr with R 
library(sparklyr)

src_tbls()
spark_conn <- spark_connect(master = "local")


Data <- read.csv("bank.csv",sep = ";") %>% 
  data.frame()


copy_to(spark_conn, Data)

track_metadata_tbl <- tbl(spark_conn, "Data")

# See how big the dataset is
object_size("Data")

# See how small the tibble is
dim(track_metadata_tbl)

# Print 5 rows, all columns
print(track_metadata_tbl,n=5)

# Examine structure of tibble
str(track_metadata_tbl)

# Examine structure of data
glimpse(track_metadata_tbl)


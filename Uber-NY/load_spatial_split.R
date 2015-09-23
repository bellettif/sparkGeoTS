library(xts)

inputFolder = '/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20/'
outputFile = '/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20_merged.csv'

allFiles = paste(inputFolder, list.files(inputFolder, pattern = "*.csv"), sep ="")

allDataFrames = lapply(allFiles, read.csv)

deleteFirstColumn <- function(df){
  df$count = 1
  return(df[,2:ncol(df)])
}

toXts <- function(df){
  return(xts(df$count, order.by = as.POSIXct(as.character(df[,1]))))
}

alignXts <- function(df){
  df <- period.apply(df, endpoints(df, "minutes", 1), sum, na.rm = TRUE )
  return(align.time(df[endpoints(df, "minutes", 1)], n=60))
}

allDataFrames = lapply(allDataFrames, deleteFirstColumn)
allDataFrames = lapply(allDataFrames, toXts)
allDataFrames = lapply(allDataFrames, alignXts)

mergedDataFrame = allDataFrames[[1]]
for(i in 2:length(allDataFrames)){
  mergedDataFrame = merge.xts(mergedDataFrame, allDataFrames[[i]], join = "outer", all = TRUE)
}

mergedDataFrame[is.na(mergedDataFrame)] <- 0

write.zoo(mergedDataFrame, outputFile, sep = '')

print(colMeans(mergedDataFrame)(0, 0))

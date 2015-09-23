library(xts)
library(lubridate)
library(plyr)

outputFolder = '/users/cusgadmin/traffic_data/uber-ny/uber_spatial_bins_20x20/'

targetData <- df.apr
targetData$Timestamp = strptime(targetData$Timestamp, format = "%m/%d/%Y %H:%M:%S")
targetData$second    = as.integer(as.POSIXct(targetData$Timestamp))
targetData$second    = targetData$second - min(targetData$second)

maxLat = quantile(targetData$Lat, 0.9)
minLat = quantile(targetData$Lat, 0.1)

maxLon = quantile(targetData$Lon, 0.9)
minLon = quantile(targetData$Lon, 0.1)

targetData <- targetData[targetData$Lon <= maxLon,]
targetData <- targetData[targetData$Lon >= minLon,]

targetData <- targetData[targetData$Lat <= maxLat,]
targetData <- targetData[targetData$Lat >= minLat,]

maxDateTime = max(targetData$second)
minDateTime = min(targetData$second)

nBinsLon  = 20
nBinsLat  = 20

lonIntervals  = seq(minLon, maxLon, length.out = nBinsLon)
latIntervals  = seq(minLat, maxLat, length.out = nBinsLat)

targetData$lonBin  <- cut(targetData$Lon, 
                          include.lowest = TRUE, 
                          breaks = lonIntervals, 
                          labels = 1:(length(lonIntervals) - 1))

targetData$latBin  <- cut(targetData$Lat, 
                          include.lowest = TRUE, 
                          breaks = latIntervals, 
                          labels = 1:(length(latIntervals) - 1))

targetData$bin = paste(targetData$lonBin, targetData$latBin, sep = "_")

binStrings = unique(targetData$bin)

for(binString in binStrings){
  write.csv(targetData[targetData$bin == binString, c("Timestamp", "Lat", "Lon", "Base")],
            file = paste(outputFolder, paste(binString, ".csv", sep = ""), sep = ""))
}
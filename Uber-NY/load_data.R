folderPath = '/users/cusgadmin/traffic_data/uber-ny/uber-trip-data/'

df.apr <- read.csv(paste(folderPath, 'uber-raw-data-apr14.csv', sep = ""), header = TRUE)
df.may <- read.csv(paste(folderPath, 'uber-raw-data-may14.csv', sep = ""), header = TRUE)
df.jun <- read.csv(paste(folderPath, 'uber-raw-data-jun14.csv', sep = ""), header = TRUE)
df.jul <- read.csv(paste(folderPath, 'uber-raw-data-jul14.csv', sep = ""), header = TRUE)
df.aug <- read.csv(paste(folderPath, 'uber-raw-data-aug14.csv', sep = ""), header = TRUE)

df.inSample <- rbind(df.apr, rbind(df.may, rbind(df.jun, rbind(df.jul, df.aug))))

df.outOfSample <- read.csv(paste(folderPath, 'uber-raw-data-sep14.csv', sep = ""), header = TRUE)


library(ggmap)
library(dplyr)
library(ggplot2)

#p = ggplot() + geom_point(data = rawData, aes(x = Lon, y = Lat))
#print(p)

map.NY <- qmap("manhattan", zoom = 12, source="osm", maptype="toner",darken = c(.3,"#BBBBBB"))
#map.NY

#p = map.NY + geom_point(data = rawData, aes(x=Lon, y=Lat), color = "dark blue", alpha = 0.01, size = 1.0)
#print (p)

p = map.NY + stat_density2d(data = df.may, aes(x=Lon, y=Lat
                                               ,color=..density..
                                               ,size=..density..#ifelse(..density..<=1,0,..density..)
                                               ,alpha=..density..),
                            geom="tile",contour=F) +
  scale_color_continuous(low="orange", high="red", guide = "none") +
  scale_size_continuous(range = c(0, 3), guide = "none") +
  scale_alpha(range = c(0,.5), guide="none") +
  ggtitle("Uber demand New York May 2014")
print(p)
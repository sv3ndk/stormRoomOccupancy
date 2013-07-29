############################
# plot the cafetaria timeline 

setwd("/Users/svend/dev/playgrounds/stormRoomOccupancy/src/main/R")

# all timelines (one row per hour => several rows are necessary to have the full history of a room)
colnames.minutes = paste("m", seq(0,59), sep="")
colnames.all <- c("roomId", "startTime", colnames.minutes)
timelines <- read.table("../../../data/timelines.csv", header=FALSE, sep=",", col.names=colnames.all)

# minute by minute occupancy of cafetaria
cafetaria <- timelines[timelines$roomId == "Cafetaria",]
cafetaria <- cafetaria[order(cafetaria$startTime),]
caf.occupancies <- c(t(cafetaria[,colnames.minutes]))

# plot
library("ggplot2")
occupancies <- data.frame(cafetaria=caf.occupancies, time=seq(1,length(caf.occupancies)))

ggplot(occupancies) + 
    geom_line(aes(time, cafetaria)) +
    scale_y_discrete ("Number of people", breaks = seq(0,35, 5)) +
    scale_x_discrete("Time", breaks = seq(1, 540, 60), labels=paste (seq(9,17), ":00", sep=""))  +
    ggtitle("Cafetaria occupancy") +
    theme(text = element_text(size=24)) 

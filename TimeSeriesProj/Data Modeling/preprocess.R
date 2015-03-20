### Loading the dataset

USDSGD <- read.csv("USDSGD.csv", header=F)

load("153_USDSGD.RData")
colnames(USDSGD) = c("Date","Bid.Price","Ask.Price","Bid.Volume","Ask.Volume")
summary(USDSGD)
# Date           Bid.Price       Ask.Price       Bid.Volume        Ask.Volume     
# 2008.09.28 23:26:47.937:      1   Min.   :1.199   Min.   :1.199   Min.   :  0.000   Min.   :  0.000  
# 2008.09.28 23:31:35.671:      1   1st Qu.:1.251   1st Qu.:1.251   1st Qu.:  1.130   1st Qu.:  1.130  
# 2008.09.28 23:32:08.903:      1   Median :1.269   Median :1.269   Median :  1.500   Median :  1.500  
# 2008.09.28 23:32:21.682:      1   Mean   :1.271   Mean   :1.271   Mean   :  1.997   Mean   :  2.007  
# 2008.09.28 23:32:25.159:      1   3rd Qu.:1.289   3rd Qu.:1.289   3rd Qu.:  2.250   3rd Qu.:  2.250  
# 2008.09.28 23:33:17.334:      1   Max.   :1.515   Max.   :1.516   Max.   :127.600   Max.   :352.400  
# (Other)                :6781671      

### Plot raw data

plot(Bid.Price,type="l")
newdates = strptime(USDSGD$Date, "%Y.%m.%d %H:%M:%S",tz="")
data = data.frame("Date"=newdates, "Bid"=USDSGD$Bid.Price)
save(data, file="data.rda")


### Process data

day = strftime(Date, format="%Y-%m-%d")
bidByDay = aggregate(Bid, by=list(day), FUN=mean)

temp=bidByDay[18:640,]
BidDay = data.frame(temp[,1], temp[,2])
colnames(BidDay) = c("day", "bid")
save(BidDay, file="BidDay.rda")

day2 = as.Date(BidDay$day, format="%Y-%m-%d")
plot(BidDay$bid ~ day2, type="l", xlab="Date", ylab="Bid Price")














data = USDSGD[186:12676, c(2,3,4,5,6)]
colnames(data) = c("Date", "BidPrice", "sd", "max", "min")
rownames(data) = c()

firsthalf = data[1:6592,]
train1 = firsthalf[1:5533,]
predict1 = firsthalf[5534:6592,]

train_full = data[1:11315,]
test_full = data[11316:12491,]


firsthalf$Date = as.character(firsthalf$Date)
firsthalf$BidPrice = as.numeric(as.character(firsthalf$BidPrice))
firsthalf$sd = as.numeric(as.character(firsthalf$sd))
firsthalf$max = as.numeric(as.character(firsthalf$max))
firsthalf$min = as.numeric(as.character(firsthalf$min))

# Plotting full data and exploration
plot(data$BidPrice, ylab = "Bid Price", type="l")
title("Aggregated USDSGD tick data for August 2010 to August 2012")

par(mfrow=c(1,2))
plot(data$BidPrice, x=zlag(data$BidPrice))
plot(data$sd, x=zlag(data$sd))
title("Scatterplot of Bid Price / sd with previous hour's Bid Price / sd", outer=TRUE, line=-2)
acf(y_half,5000)

y_full = train_full$BidPrice
y_full_test = test_full$BidPrice
y_train = data$BidPrice
y_train2 = firsthalf$BidPrice

summary(train1)
plot(train1$BidPrice, ylab = "Bid Price", type="l")
title("Aggregated USDSGD tick data for August 2010 to June 2011")
y_half = train1$BidPrice
y_half_test = predict1$BidPrice

par(mfrow=c(2,1))
par(mfrow=c(1,1))


# Results
BIC(linear_mod)
BIC(season_model)
BIC(har_model)

BIC(linear_mod2)
BIC(har_model2)



save(data, file="USDSGDdata.Rda")
write.csv(file="test_full.csv", x=test_full)
write.csv(file="harmonic_predictions_full.csv", x=pred.hmodfull)


par(mfrow=c(1,2))
qqnorm(rstudent(linear_mod2), main="QQ Plot of Linear model")
qqline(rstudent(linear_mod2))
qqnorm(rstudent(har_model2), main="QQ Plot of Linear harmonic model")
qqline(rstudent(har_model2))

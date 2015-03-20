#Harmonic model
months_full = as.factor(substr(data$Date, 6,7))
days_full = as.factor(substr(data$Date,1,10))
table(days_full)
har_freq=480*6

#Half
ts_half = ts(y_half, frequency=har_freq, start=1)
time_ts_half = time(ts_half)

har_half. = harmonic(ts_half, 1)
har_model = lm(ts_half ~ time_ts_half + har_half.)
summary(har_model)

par(mfrow=c(2,1))
plot(fitted(har_model), type="l", ylab="Bid Price", 
     main="Bid Price with Linear Harmonic Time Trend from Aug 2010 to June 2011", 
     ylim = range(c(har_model$fitted.values, y_half)))
lines(y_half)

acf(rstudent(har_model), 5000, main="Sample autocorrelation of residuals of harmonic model")
title("Sample autocorrelation of residuals of linear model")
plot(rstudent(har_model), type="l")


  #season
har_freq=480*6
ts_half = ts(y_half, frequency=har_freq, start=1)
season_ts_half = season(ts_half)
season_model = lm(ts_half ~ time_ts_half + season_ts_half)
summary(season_model)
plot(fitted(season_model), type="l", ylab="Bid Price", col="red", 
     main="Bid Price with Linear Seasonal Time Trend from Aug 2010 to June 2011", 
     ylim = range(c(season_model$fitted.values, y_half)))
lines(y_half)

ts_all = ts(y_train2, frequency=har_freq, start=1)
season_ts_half_test = season(ts_all)[5534:6592]
new = data.frame(time_ts_half=time_ts_half_predict, season_ts_half=season_ts_half_test)
pred.smod = predict(lm(ts_half ~ time_ts_half + season_ts_half), new)

plot(y_half, type="l", ylab="Bid Price", main="Bid Price with Linear and Seasonal Time Trend with Predictions",
     ylim = range(c(y_half, pred.smod)), xlim=c(1,6592))
lines(y=predict1$BidPrice, x=seq(5534,6592), col="green")
lines(y=season_model$fitted.values, x=seq(1:5533), col="red")
lines(y=pred.smod, x=seq(5534,6592), col="red", lty=2)
legend("topright", legend=c("Train", "Test", "Fitted Values", "Predictions"), lty=c(1,1,1,2), col=c("black","green","red","red"))




  #linear de-trended
ts_half_detrend = ts(linear_half_detrend, frequency=har_freq, start=1)
time_ts_half_detrend = time(ts_half_detrend)

har_half_detrend. = harmonic(ts_half_detrend, 1)
har_half_detrend_model = lm(ts_half_detrend ~ time_ts_half_detrend + har_half_detrend.)
summary(har_half_detrend_model)
plot(fitted(har_half_detrend_model), type="l", ylim = range(c(har_half_detrend_model$fitted.values, linear_half_detrend)))
lines(linear_half_detrend)

#Predict

coeff = har_model$coefficients
pred.hmod = c()
ts_half_test = ts(y_half_test, frequency=har_freq, start=time_ts_half[5533])

time_ts_half_predict = time(ts_half_test)
har_half_predict. = harmonic(ts_half_test,1)

for (i in 1:(length(y_train2)-length(y_half))) {
  t = time_ts_half_predict[i]
  har_row = har_half_predict.[i,]
  pred.hmod[i] = coeff[1] + coeff[2]*t + coeff[3]*har_row[1] + coeff[4]*har_row[2]  
}
par(mfrow=c(1,1))
plot(y_half, type="l", ylab="Bid Price", main="Bid Price with Linear Harmonic Time Trend with Prediction (Half set)",
     ylim = range(c(y_half, pred.hmod)), xlim=c(1,6592))
lines(y=pred.hmod, x=seq(5534,6592), col="red", lty=2)
lines(y=predict1$BidPrice, x=seq(5534,6592), col="green")
lines(y=har_model$fitted.values, x=seq(1:5533), col="red")
legend("topright", legend=c("Train", "Test", "Fitted Values", "Predictions"), lty=c(1,1,1,2), col=c("black","green","red","red"))

difference_half = abs(predict1$BidPrice - pred.hmod) / predict1$BidPrice

deviation_all = abs(data$BidPrice - zlag(data$BidPrice)) / y_train

#Full
har_freq2 = 480*18
ts_full = ts(y_full, frequency=har_freq2, start=1)
har_full. = harmonic(ts_full, 1)
time_ts_full = time(ts_full)
har_model2 = lm(ts_full ~ time_ts_full + har_full.)
summary(har_model2)

plot(fitted(har_model2), type="l", ylab="Bid Price", 
     main="Bid Price with Linear Harmonic Time Trend from Aug 2010 to May 2012", 
     ylim = range(c(har_model2$fitted.values, y_full)))
lines(y_full)

acf(rstudent(har_model2), 10000, main="Sample autocorrelation of residuals of harmonic model")
title("Sample autocorrelation of residuals of linear model")
plot(rstudent(har_model2), type="l", main="Standardized residuals of linear + harmonic model")
lines(lowess(1:11315, rstudent(har_model2)), type="l", col="red")

qqPlot(rstudent(har_model2))

  #Predict
season_ts_full = season(ts_full)
season_model_full = lm(ts_full ~ time_ts_full + season_ts_full)
summary(season_model_full)
plot(fitted(season_model_full), type="l", ylab="Bid Price", col="red", 
     main="Bid Price with Linear Seasonal Time Trend from Aug 2010 to June 2011", 
     ylim = range(c(season_model_full$fitted.values, y_full)))
lines(y_full)

ts_all = ts(y_train, frequency=har_freq2, start=1)
season_ts_full_test = season(ts_all)[length(y_full)+1,length(y_train)]
new = data.frame(time_ts_full=time_ts_full_predict, season_ts_full=season_ts_full_test)
pred.smodfull = predict(lm(ts_full ~ time_ts_full + season_ts_full), new)

plot(y_full, type="l", ylab="Bid Price", main="Bid Price with Linear and Seasonal Time Trend with Prediction",
     ylim = range(c(y_full, pred.smodfull)), xlim=c(1,length(y_train)))
lines(y=y_full_test, x=seq(length(y_full)+1,length(y_train)), col="green")
lines(y=season_model_full$fitted.values, x=seq(1:length(y_full)), col="red")
lines(y=pred.smodfull, x=seq(length(y_full)+1,length(y_train)), col="red", lty=2)
legend("topright", legend=c("Train", "Test", "Fitted Values", "Predictions"), lty=c(1,1,1,2), col=c("black","green","red","red"))

BIC(season_model_full)


#Predict
coeff = har_model2$coefficients
pred.hmodfull = c()
ts_full_test = ts(y_full_test, frequency=har_freq2, start=time_ts_full[11315])

time_ts_full_predict = time(ts_full_test)
har_full_predict. = harmonic(ts_full_test,1)

for (i in 1:(length(y_train)-length(y_full))) {
  t = time_ts_full_predict[i]
  har_row = har_full_predict.[i,]
  pred.hmodfull[i] = coeff[1] + coeff[2]*t + coeff[3]*har_row[1] + coeff[4]*har_row[2]  
}
par(mfrow=c(1,1))
plot(y_full, type="l", ylab="Bid Price", main="Bid Price with Linear Harmonic Time Trend with Prediction (Full set)",
     ylim = range(c(y_full, pred.hmodfull)), xlim=c(1,12491))
lines(y=pred.hmodfull, x=seq(11316,12491), col="red", lty=2)
lines(y=test_full$BidPrice, x=seq(11316,12491), col="green")
lines(y=har_model2$fitted.values, x=seq(1:11315), col="red")
legend("topright", legend=c("Train", "Test", "Fitted Values", "Predictions"), lty=c(1,1,1,2), col=c("black","green","red","red"))

difference = abs(test_full$BidPrice - pred.hmodfull) / test_full$BidPrice

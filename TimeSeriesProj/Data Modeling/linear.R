# Linear model
library(TSA)

#Full
par(mfrow=c(2,1))
linear_mod2 = lm(y_full ~ time(y_full))
plot(y_full, type="l", ylab="Bid Price", main="Bid Price with Linear Time Trend from Aug 2010 to May 2012")
abline(linear_mod2)

summary(linear_mod2)

linear_full_detrend = y_full - linear_mod2$fitted.values
plot(linear_full_detrend, type="l")
plot(rstudent(linear_mod2), type="l", main="Standardized residuals of linear model")
lines(lowess(1difference_half = abs(predict1$BidPrice - pred.lmod)
:11315, rstudent(linear_mod2)), type="l", col="red")

acf(rstudent(linear_mod2), 10000, main="Sample autocorrelation of residuals of linear model")
title("Sample autocorrelation of residuals of linear model")


#Half
time_half = time(y_half)
linear_mod = lm(y_half ~ time_half)
plot(y_half, type="l", ylab="Bid Price", main="Bid Price with Linear Time Trend from Aug 2010 to June 2011")
abline(linear_mod)

linear_half_detrend = y_half - linear_mod$fitted.values
plot(linear_half_detrend, type="l")
boxplot(linear_half_detrend~season(ts_half))
hist(rstudent(linear_mod))
plot(rstudent(linear_mod), type="l")

acf(rstudent(linear_mod), 5000, main="Sample autocorrelation of residuals of linear model")
title("Sample autocorrelation of residuals of linear model")
BIC(linear_mod)


#Predict
summary(linear_mod)
new = data.frame(time_half=seq(5534,6592))
pred.lmod = predict(lm(y_half ~ time_half), new)

plot(y_half, type="l", ylab="Bid Price", main="Bid Price with Linear Time Trend with Prediction",
     ylim = range(c(y_half, pred.lmod)), xlim=c(1,6592))
lines(y=pred.lmod, x=seq(5534,6592), col="red", lty=2)
lines(y=predict1$BidPrice, x=seq(5534,6592), col="green")
lines(y=linear_mod$fitted.values, x=seq(1:5533), col="red")
legend("topright", legend=c("Train", "Test", "Fitted Values", "Predictions"), lty=c(1,1,1,2), col=c("black","green","red","red"))

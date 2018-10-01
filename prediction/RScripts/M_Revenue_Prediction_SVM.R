##################################
#
# Project: Hotel Revenue Modeling
#  Author: Michael Heinzer & Daniel Mueller
# Version: 1.0
#    Date: 12.03.2018 
# 
# Summary: Modelling the revenue prediction for multiple linear regression
#
# Working on ETH machine? You need to start R-Studio as Administrator

###################################


# 0. clear workspace
####################
rm(list = ls(all=TRUE))


# 1. set working directory and load libraries
###################
library(ISLR)
library(lattice)
library(ggplot2)
library(randomForest)
library(mice)
library(caret)
require(xgboost)
require(dummy)
rm(list = ls(all=TRUE))
set.seed(541)

# 2. data import & structural check
###################################
exclude_outliers <- TRUE
percentage_exclude <- 0.05 # overall
show_plots <- FALSE
nb_cross_validation <- 5
nb_imputations <- 20
param_nb_steps <- 15
param_eta <- 0.2
param_depth <- 2

# Data reading and representation in R
hotels <- read.csv(file="C:/Users/heinzerm/Google Drive/MasterThesis-PCSWTHOOR/webscraping/prediction/revenue_prediction_all.csv", header=TRUE, sep=",", encoding = 'UTF-8-BOM')


# Remove lines where certain attributes have NA values
hotels <- hotels[complete.cases(hotels[ , c('go_reviewcount')]),]

# Calculate real revenue for better comparison
hotels$revenue <- hotels$revenue*1920


# Exclude outliers? Exclude top 10 and last 10 TODO move this later
if (exclude_outliers) {
  hotels <- hotels[order(hotels$revenue, decreasing = TRUE),]
  to_remove <- round(length(hotels$revenue)*(percentage_exclude /2))
  hotels <- hotels[(to_remove+1):(length(hotels$revenue)-to_remove),]
  # Everyday I'm shuffeling
  hotels <- hotels[sample(nrow(hotels)),]
}

# Impute variables where we do not have a special model for prediction (all except for rooms and price)
pMiss <- function(x){sum(is.na(x))/length(x)*100}
count_missing <- apply(hotels,2,pMiss)
missing <- count_missing[count_missing > 0]
complete <- count_missing[count_missing == 0]
missing <- names(missing)
complete <- names(complete)
exclude_from_imputation <- c("rooms", "price")
for_imputation <- setdiff(missing, exclude_from_imputation)

# Create list with top15 predictors for price from hotel dataset
load("priceFit_hotel.rda")
gbmImp <- varImp(priceFit, scale = FALSE)
importance <- gbmImp$importance
importance$names <- rownames(importance)
importance <- importance[order(importance$Overall, decreasing = TRUE),]
imp15price <- importance[1:15,]$names


# Create list for top15 predictors for room from hotel dataset
load("roomFit_hotel.rda")
gbmImp <- varImp(roomFit, scale = FALSE)
importance <- gbmImp$importance
importance$names <- rownames(importance)
importance <- importance[order(importance$Overall, decreasing = TRUE),]
imp15rooms <- importance[1:15,]$names

# Get top25 predictors for the missing data
top25 <- union(imp15price, imp15rooms)
inters <- intersect(top25, for_imputation) # has to be empty, important!
imputation_columns <- union(for_imputation, top25)





# Arrays for storage
test_rmses <- array(data = NA, dim = c(nb_imputations * nb_cross_validation))
train_rmses <- array(data = NA, dim = c(nb_imputations * nb_cross_validation))
test_maes <- array(data = NA, dim = c(nb_imputations * nb_cross_validation))
train_maes <- array(data = NA, dim = c(nb_imputations * nb_cross_validation))
test_rsqs <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))
test_rss <-array(data = NA, dim=c(nb_imputations * nb_cross_validation))
train_means <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))
test_mean_rmes <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))
test_mean_maes <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))
fits <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))
train_rsqs <- array(data = NA, dim=c(nb_imputations * nb_cross_validation))

# Function for Root Mean Squared Error
rmse <- function(error) { sqrt(mean(error^2)) }
# Function for Mean Absolute Error
mae <- function(error) { mean(abs(error)) }


# Train/Test split before we do imputation to avoid information leaking
# Load models for impuation
load("roomFit_tripadvisor.rda")
load("priceFit_hotel.rda")

#Randomly shuffle the data
hotels<-hotels[sample(nrow(hotels)),]
#Create nb_cross_validation equally size folds
folds <- cut(seq(1,nrow(hotels)),breaks=nb_cross_validation,labels=FALSE)
#Perform 10 fold cross validation
for(j in 1:nb_cross_validation){
  #Segement data by fold using the which() function 
  testIndexes <- which(folds==j,arr.ind=TRUE)
  test_hotels <- hotels[testIndexes, ]
  train_hotels <- hotels[-testIndexes, ]
  
  # Do the actual imputation for the reduced and split datasets
  test_hotels_imp <- test_hotels[,imputation_columns]
  imputed_test_hotels <- mice(test_hotels_imp, pri = FALSE, m = nb_imputations)
  train_hotels_imp <- train_hotels[,imputation_columns]
  imputed_train_hotels <- mice(train_hotels_imp, pri = FALSE, m = nb_imputations)
  
  if (show_plots){
    # Plot the densities of the imputed data
    densityplot(train_hotels_imp)
  }
  
  # Fit a model and calculate the error for all imputed datasets
  for (i in 1:nb_imputations){
    completed_test_data <- complete(imputed_test_hotels,i)
    completed_train_data <- complete(imputed_train_hotels,i)
    # Replace all the imputed columns with the new data
    test_hotels[, for_imputation] <- completed_test_data[, for_imputation]
    train_hotels[, for_imputation] <- completed_train_data[, for_imputation]
    # Now only rooms and price are missing
    colnames(train_hotels)[colSums(is.na(train_hotels)) > 0]
    
    # Impute missing rooms from model for train set
    train_hotels$rooms.pre <- predict(roomFit,newdata = train_hotels)
    train_hotels$rooms.imp <- ifelse(is.na(train_hotels$rooms), train_hotels$rooms.pre, train_hotels$rooms)
    train_hotels$rooms <- train_hotels$rooms.imp
    train_hotels <- subset(train_hotels, select = -c(rooms.imp,rooms.pre))
    # Impute missing prices from model for train set
    train_hotels$price.pre <- predict(priceFit,newdata = train_hotels)
    train_hotels$price.imp <- ifelse(is.na(train_hotels$price), train_hotels$price.pre, train_hotels$price)
    train_hotels$price <- train_hotels$rooms.imp
    train_hotels <- subset(train_hotels, select = -c(price.imp, price.pre))
    
    # Impute missing rooms from model for test set
    test_hotels$rooms.pre <- predict(roomFit,newdata = test_hotels)
    test_hotels$rooms.imp <- ifelse(is.na(test_hotels$rooms), test_hotels$rooms.pre, test_hotels$rooms)
    test_hotels$rooms <- test_hotels$rooms.imp
    test_hotels <- subset(test_hotels, select = -c(rooms.imp,rooms.pre))
    # Impute missing prices from model for test set
    test_hotels$price.pre <- predict(priceFit,newdata = test_hotels)
    test_hotels$price.imp <- ifelse(is.na(test_hotels$price), test_hotels$price.pre, test_hotels$price)
    test_hotels$price <- test_hotels$rooms.imp
    test_hotels <- subset(test_hotels, select = -c(price.imp, price.pre))
    
    # Nothing should be missing now, no more N/A values
    colnames(train_hotels)[colSums(is.na(train_hotels)) > 0]
    
    # Create new variables which intuitively make sense
    train_hotels$ed_room_occupancy <- train_hotels$ed_room_occupancy/100
    train_hotels$occupied_rooms <- train_hotels$rooms * train_hotels$ed_room_occupancy # Improves accuracy by 30k
    train_hotels$stays_per_room <- train_hotels$ed_room_stays/train_hotels$ed_rooms # Seems to not do much
    train_hotels$reviews_per_room <- train_hotels$go_reviewcount/train_hotels$rooms # not much
    
    # Create new variables which intuitively make sense
    test_hotels$ed_room_occupancy <- test_hotels$ed_room_occupancy/100
    test_hotels$occupied_rooms <- test_hotels$rooms * test_hotels$ed_room_occupancy # Improves accuracy by 30k
    test_hotels$stays_per_room <- test_hotels$ed_room_stays/test_hotels$ed_rooms # Seems to not do much
    test_hotels$reviews_per_room <- test_hotels$go_reviewcount/test_hotels$rooms # not much
    
    
    # Get mean for comparison
    mean_train <- mean(train_hotels$revenue)
    mean_test <- mean(test_hotels$revenue)
    train_means[((j-1)*nb_imputations+i)] <- mean_train
    
    algo <- 'svmRadialSigma'
    
    
    
    # run model
    fitControl <- trainControl(## 10-fold CV
      method = "repeatedcv",
      number = 5,
      ## repeated three times
      repeats = 1)
    
    revenueFit <- train(revenue ~ ., data = train_hotels, method = algo, tuneLength = 3)
    
    
    # Show diagnostics
    fits[((j-1)*nb_imputations+i)] <- revenueFit
    if (show_plots){
      plot(revenueFit, main = algo)
    }
    
    # Predict the test set from fitted model
    test_prediction <- predict(revenueFit, newdata = test_hotels)
    # fiddle: only to calculate model fit for test sample
    train_prediction <- predict(revenueFit, newdata = train_hotels)
    
    # Calculate error for Test
    error_test <- test_hotels$revenue - test_prediction
    # Calculate error for Train
    error_train <- train_hotels$revenue - train_prediction
    
    # Mean model Test error
    mean_error_test <- mean_train-test_hotels$revenue
    # Mean model Train error
    mean_error_train <- mean_train-train_hotels$revenue
    
    cat("Run CV #", j, " of ", nb_cross_validation, " and imputation part #", i, " of " , nb_imputations, "\n")
    cat("The Test root-mean-square error (RMSE) is: ", rmse(error_test), "Model:", algo, "\n")
    cat("The Train root-mean-square error (RMSE) is: ", rmse(error_train), "Model:", algo, "\n")
    cat("The simple mean RMSE Test error is:", rmse(mean_error_test), "Model: Simple mean\n")
    cat("The simple mean RMSE Train error is:", rmse(mean_error_train), "Model: Simple mean\n")
    ##############################
    cat("The Test mean absolute error is (MAE): ", mae(error_test), "Model:", algo, "\n")
    cat("The Train mean absolute error is (MAE): ", mae(error_train), "Model:", algo, "\n")
    cat("The simple mean Test mean absolute error is (MAE): ", mae(mean_error_test), "Model: Simple mean\n")
    cat("The simple mean Train mean absolute error is (MAE): ", mae(mean_error_train), "Model: Simple mean\n")
    ##############################
    
    
    
    # calculate R-square: for Test (Algo)
    rss <- sum((test_hotels$revenue - test_prediction) ^ 2)
    tss <- sum((test_hotels$revenue - mean_test) ^ 2)
    rsq <- 1 - rss/tss
    test_rsqs[((j-1)*nb_imputations+i)] <- rsq
    test_rss[((j-1)*nb_imputations+i)] <- rss
    cat("The Test R-square for Model:", algo, "is", rsq, "\n")
    # calculate R-square: for Test
    rss <- sum((train_hotels$revenue - train_prediction) ^ 2)
    tss <- sum((train_hotels$revenue - mean_train) ^ 2)
    rsq <- 1 - rss/tss
    cat("The Train R-square for Model:", algo, "is", rsq, "\n")
    train_rsqs[((j-1)*nb_imputations+i)] <- rsq
    ##############################
    # calculate R-square: for Test (Simple mean) What is the point of this??? test_hotels$revenue is canceled out
    rss <- sum((test_hotels$revenue - mean_train-test_hotels$revenue) ^ 2)
    tss <- sum((test_hotels$revenue - mean_test) ^ 2)
    rsq <- 1 - rss/tss
    cat("The Test R-square for Model: Simple mean is", rsq, "\n")
    # calculate R-square: for Train
    rss <- sum((train_hotels$revenue - mean_train-train_hotels$revenue) ^ 2)
    tss <- sum((train_hotels$revenue - mean(train_hotels$revenue)) ^ 2)
    rsq <- 1 - rss/tss
    cat("The Train R-square for Model: Simple mean is", rsq, "\n")
    
    
    cat("\n\n")
    
    
    # Store RMSE and MAE for plots
    test_rmses[((j-1)*nb_imputations+i)] <- rmse(error_test)
    train_rmses[((j-1)*nb_imputations+i)] <- rmse(error_train)
    test_mean_rmes[((j-1)*nb_imputations+i)] <- rmse(mean_error_test)
    test_maes[((j-1)*nb_imputations+i)] <- mae(error_test)
    train_maes[((j-1)*nb_imputations+i)] <- mae(error_test)
    test_mean_maes[((j-1)*nb_imputations+i)] <- mae(mean_error_test)
    
  }
}




# Create data:
x=c(1:(nb_cross_validation*nb_imputations))
b <- test_rmses
c <-test_mean_rmes
d <- train_means

# Make a basic graph
plot( b~x , type="b" , bty="l" , xlab="Dataset" , ylab="RMSE/Revenue" , col=rgb(0.2,0.4,0.1,0.7) , lwd=3 , pch=17 , ylim=c(1,2000000), main ="RMSE for Revenue Estimation by SVM")
lines(c ~x , col=rgb(0.8,0.4,0.1,0.7) , lwd=3 , pch=19 , type="b" )
lines(d ~x , col=rgb(0.2,0.1,0.8,0.7) , lwd=3 , pch=15 , type="b" )

# Add a legend
legend("bottomleft", 
       legend = c("RMSE SVM", "RMSE Mean Prediction", "Mean Revenue"), 
       col = c(rgb(0.2,0.4,0.1,0.7), 
               rgb(0.8,0.4,0.1,0.7), rgb(0.2,0.1,0.8,0.7)), 
       pch = c(17,19,15), 
       bty = "n", 
       pt.cex = 2, 
       cex = 1.2, 
       text.col = "black", 
       horiz = F , 
       inset = c(0.1, 0.1))


# Create data:
x=c(1:(nb_cross_validation*nb_imputations))
b <- test_maes
c <-test_mean_maes
d <- train_means

# Make a basic graph
plot( b~x , type="b" , bty="l" , xlab="Dataset" , ylab="MAE/Revenue" , col=rgb(0.2,0.4,0.1,0.7) , lwd=3 , pch=17 , ylim=c(0,2000000), main ="MAE for Revenue Estimation by SVM")
lines(c ~x , col=rgb(0.8,0.4,0.1,0.7) , lwd=3 , pch=19 , type="b" )
lines(d ~x , col=rgb(0.2,0.1,0.8,0.7) , lwd=3 , pch=15 , type="b" )

# Add a legend
legend("bottomleft", 
       legend = c("MAE SVM", "MAE Mean Prediction", "Mean Revenue"), 
       col = c(rgb(0.2,0.4,0.1,0.7), 
               rgb(0.8,0.4,0.1,0.7), rgb(0.2,0.1,0.8,0.7)), 
       pch = c(17,19,15), 
       bty = "n", 
       pt.cex = 2, 
       cex = 1.2, 
       text.col = "black", 
       horiz = F , 
       inset = c(0.1, 0.1))

# Create data:
x=c(1:(nb_cross_validation*nb_imputations))
b <- test_rmses - train_rmses
c <-test_rmses
d <- train_rmses

# Make a basic graph
plot( b~x , type="b" , bty="l" , xlab="Dataset" , ylab="RMSE" , col=rgb(0.2,0.4,0.1,0.7) , lwd=3 , pch=17 , ylim=c(0,2000000), main ="RMSE for Training and Test set")
lines(c ~x , col=rgb(0.8,0.4,0.1,0.7) , lwd=3 , pch=19 , type="b" )
lines(d ~x , col=rgb(0.2,0.1,0.8,0.7) , lwd=3 , pch=15 , type="b" )

# Add a legend
legend("bottomleft", 
       legend = c("Test Error - Train Error", "Test Error", "Train Error"), 
       col = c(rgb(0.2,0.4,0.1,0.7), 
               rgb(0.8,0.4,0.1,0.7), rgb(0.2,0.1,0.8,0.7)), 
       pch = c(17,19,15), 
       bty = "n", 
       pt.cex = 2, 
       cex = 1.2, 
       text.col = "black", 
       horiz = F , 
       inset = c(0.1, 0.2))


  # Variable importance plot
gbmImp <- varImp(revenueFit, scale = FALSE)
plot(gbmImp, top = 20, main="Variable Importance for SVM Revenue Estimation")



cat("Overall RMSE mean", mean(test_rmses), "\n")
cat("Overall RMSE sd", sd(test_rmses), "\n")
cat("Overall MAE mean", mean(test_maes), "\n")
cat("Overall MAE sd", sd(test_maes), "\n")
cat("Overall R2 train mean", mean(train_rsqs), "\n")
cat("Overall R2 train sd", sd(train_rsqs), "\n")
cat("Overall R2 test mean", mean(test_rsqs), "\n")
cat("Overall R2 test sd", sd(test_rsqs), "\n")


cat("Overall RMSE mean est mean ", mean(test_mean_rmes), "\n")
cat("Overall RMSE mean est sd", sd(test_mean_rmes), "\n")
cat("Overall MAE mean est mean", mean(test_mean_maes), "\n")
cat("Overall MAE mean est sd", sd(test_mean_maes), "\n")





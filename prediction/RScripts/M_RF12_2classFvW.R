# Test approaches with the full dataset
library(ISLR)
library(caret)
library(ggplot2)
library(randomForest)
library(ROSE)
library(caTools)
library(DMwR)
#set.seed(541)
# The file with averaged revenue has a lower class imbalance
use_avg <- TRUE
# Only predict, change no change
two_classes <- TRUE
# Choose the algorithm which will be used to predict classes
nb_imputations <- 100

set.seed(10)

# Data reading and representation in R
if (use_avg) {
  hotels <- read.csv(file="C:/Users/heinzerm/Google Drive/MasterThesis-PCSWTHOOR/webscraping/prediction/revenue_classification_avg.csv", header=TRUE, sep=",", encoding = 'UTF-8-BOM')
} else{
  hotels <- read.csv(file="C:/Users/heinzerm/Google Drive/MasterThesis-PCSWTHOOR/webscraping/prediction/revenue_classification_vot.csv", header=TRUE, sep=",", encoding = 'UTF-8-BOM')
}


# To factor variable
hotels$class <- factor(hotels$class)
summary(hotels$class)

# Remove lines where certain attributes have NA values
hotels <- hotels[complete.cases(hotels[ , c('go_reviewcount')]),]

# Imputation with mice package except for rooms and price
# Impute variables where we do not have a special model for prediction (all except for rooms and price)
pMiss <- function(x){sum(is.na(x))/length(x)*100}
count_missing <- apply(hotels,2,pMiss)
missing <- count_missing[count_missing > 0]
complete <- count_missing[count_missing == 0]
missing <- names(missing)
complete <- names(complete)
#exclude_from_imputation <- c("rooms", "price")
exclude_from_imputation <- c()
for_imputation <- setdiff(missing, exclude_from_imputation)

# Create list with top15 predictors for price from hotel dataset
load("priceFit_hotel.rda")
gbmImp <- varImp(priceFit, scale = FALSE)
importance <- gbmImp$importance
importance$names <- rownames(importance)
importance <- importance[order(importance$Overall, decreasing = TRUE),]
imp15price <- importance[1:20,]$names


# Create list for top15 predictors for room from hotel dataset
load("roomFit_hotel.rda")
gbmImp <- varImp(roomFit, scale = FALSE)
importance <- gbmImp$importance
importance$names <- rownames(importance)
importance <- importance[order(importance$Overall, decreasing = TRUE),]
imp15rooms <- importance[1:20,]$names

# Get top25 predictors for the missing data
top25 <- union(imp15price, imp15rooms)
inters <- intersect(top25, for_imputation) # has to be empty, important!
imputation_columns <- union(for_imputation, top25)
imputation_columns <- intersect(colnames(hotels), imputation_columns)
hotels_imp <- hotels[,imputation_columns]

# To the actual imputation for the reduced dataset
imputed_hotels <- mice(hotels_imp, pri = FALSE, m = nb_imputations)
# Plot the densities of the imputed data
fp <- array(data = NA, dim=c(nb_imputations))
fn <-array(data = NA, dim=c(nb_imputations))
tp <-array(data = NA, dim=c(nb_imputations))
tn <-array(data = NA, dim=c(nb_imputations))


# Potentially do all of the following for every i
for (i in 1:nb_imputations){
  completedData <- complete(imputed_hotels,i)
  # Replace all the imputed columns with the new data
  hotels[, for_imputation] <- completedData[, for_imputation]
  
  include <- which(hotels$class != 0)
  hotels_usable <- hotels[include,]
  hotels_usable$class <- factor(hotels_usable$class)
  
  # Split training and test set
  indxTrain <- createDataPartition(y = hotels_usable$class,p = 0.5,list = FALSE)
  imbal_train <- hotels_usable[indxTrain,]
  testing <- hotels_usable[-indxTrain,]
  table(imbal_train$class)
  # Do up sampling to remedy class imbalances
  training <- imbal_train
  #training <- upSample(x = imbal_train[, -ncol(imbal_train)], y = imbal_train$class)
  
  algo <- 'xgbTree' ## when running the first time, you need to install it
  #algo <- 'plr' # Penalized logistic regression ## when running the first time, you need to install it
  # load dataset
  
  control <- trainControl(method="cv", number=2)
  model <- train(class~., data=training, method=algo, metric="Accuracy", trControl=control)
  predictions <- predict(model, newdata=testing)
  cm <- confusionMatrix(predictions, testing$class)
  
  tp[i] <- cm$table[2,2]
  fn[i] <- cm$table[1,2]
  fp[i] <- cm$table[2,1]
  tn[i] <- cm$table[1,1]
  cat("Iteration", i, "\n")
  
}

cm$table[2,2] <- mean(tp)
cm$table[1,2] <- mean(fn)
cm$table[2,1] <- mean(fp)
cm$table[1,1] <- mean(tn)

tp
gbmImp <- varImp(model, scale = FALSE)
plot(gbmImp, top = 20, main="Variable Importance for Boosted Trees Growth Estimation")

# estimate skill on validation dataset
set.seed(9)

fits[1]
fourfoldplot(cm$table, main = "Confusion Matrix for Growth Prediction Boosted Trees")


accuracy <- (tp+tn)/(tp+tn+fp+fn)
mean(accuracy)
sd(accuracy)
precision <- tp / (tp+fp)
mean(precision)
sd(precision)
recall <- tp / (tp+fn)
mean(recall)
sd(recall)
f1 <- 2 / (1/precision + 1/recall)
mean(f1)
sd(f1)
baseline <- (tp+fn) / (tp+tn+fp+fn)
mean(baseline)
sd(baseline)


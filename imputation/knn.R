# KNN testing for imputation
library(ISLR)
library(caret)
set.seed(421)
hotels <- read.csv(file="C:/Users/heinzerm/Google Drive/MasterThesis-PCSWTHOOR/webscraping/prediction/revenue_prediction.csv", header=TRUE, sep=",", encoding = 'UTF-8-BOM')
# Remove lines where we don't have a value for lower price
ch <- hotels[complete.cases(hotels[ , c('ta_lower_price', 'rooms')]),]
data <- ch[, c('price', 'xn', 'yn', 'ratingvalue', 'ta_ratingvalue', 'rooms')]

#Spliting data as training and test set. Using createDataPartition() function from caret
indxTrain <- createDataPartition(y = data$price,p = 0.75,list = FALSE)
training <- data[indxTrain,]
testing <- data[-indxTrain,]


ctrl <- trainControl(method="repeatedcv",repeats = 3) #,classProbs=TRUE,summaryFunction = twoClassSummary)
knnFit <- train(price ~ xn+yn+ratingvalue, data = training, method = "knn", tuneLength = 20)
knnFit
plot(knnFit)
# Try the hold-out data
knnPredict <- predict(knnFit,newdata = testing )
#Get the confusion matrix to see accuracy value and other parameter values
# confusionMatrix(knnPredict, testing$price )
m <- mean(training$price)
m
mean(sqrt((knnPredict-testing$price)^2))
mean(sqrt((m-testing$price)^2))

summary(data$price)

plot(knnPredict-testing$price)

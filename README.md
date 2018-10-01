## Predicting the Success of Hotels with Online Ratings
This repository contains most of the scripts created to collect, clean, aggregate and process the data which was used during my master thesis.
The revenue data was provided by a swiss insurance company and is confidential. The goal of this project was to find a way to predict revenue and growth of the hotels in the dataset from publicly available data. The main data sources were:

swisshotels.ch for general hotel attributes such as size, location and amenities
tripadvisor.com for ratings and attributes
booking.com for ratings and attributes
google.com for ratings

An extensive description of the entire project can be found in the MasterThesis.pdf file.

# Python program
Python was used to collect data on hotels in our database, we crawled the websites of swisshotel, TripAdvisor and booking.com using the scrapy library. However, as these websites change their structure frequently, these scripts are most likely outdated at the time of publication.
The library pandas was used to retrieve, clean and store the scraped data. Due to the research nature of the project the code is unfortunately very unstructured and most of the main functions are contained in the DatabasePandas.py file. 

# R scripts
R scripts were used to read the clean data and create models to predict the revenue and growth of the companies.

# Data
None of the collected datasets will be publicly available.

# Understanding gender differences in professional European football through Machine Learning interpretability and match actions data.
This repository contains the full data pipeline implemented for the study *Understanding gender differences in professional European football through Machine Learning interpretability and match actions data*.
We evaluated European male, and female football players' main differential features in-match actions data under the assumption of finding significant differences and established patterns between genders. A methodology for unbiased feature extraction and objective analysis is presented based on data integration and machine learning explainability algorithms. Female (1511) and male (2700) data points were collected from event data categorized by game period and player position. Each data point included the main tactical variables supported by research and industry to evaluate and classify football styles and performance. We set up a supervised classification pipeline to predict the gender of each player by looking at their actions in the game. The comparison methodology did not include any qualitative enrichment or subjective analysis to prevent biased data enhancement or gender-related processing. The pipeline had three representative binary classification models; A logic-based Decision Trees, a probabilistic Logistic Regression and a multilevel perceptron Neural Network. Each model tried to draw the differences between male and female data points, and we extracted the results using machine learning explainability methods to understand the underlying mechanics of the models implemented. A good model predicting accuracy was consistent across the different models deployed.

## Installation

Install the required python packages
```
pip install -r requirements.txt
```

To handle heterogeneity and performance efficiently, we use PySpark from [Apache Spark](https://spark.apache.org/). PySpark enables an end-user API for Spark jobs. You might want to check how to set up a local or remote Spark cluster in [their documentation](https://spark.apache.org/docs/latest/api/python/index.html).

## Repository structure
This repository is organized as follows:

- Preprocessed data from the two different data streams is collecting in [the data folder](data/). For the Opta files, it contains the event-based metrics computed from each match of the 2017 Women's Championship and a single file calculating the event-based metrics from the 2016 Men's Championship published [here](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5).
Even though we cannot publish the original data source, the two python scripts implemented to homogenize and integrate both data streams into event-based metrics are included in [the data gathering folder](data_gathering/) folder contains the graphical images and media used for the report.
- The [data cleaning folder](data_cleaning/) contains descriptor scripts for both data streams and [the final integration](data_cleaning/merger.py)
- [Classification](classification/) contains all the Jupyter notebooks for each model present in the experiment as well as some persistent models for testing.

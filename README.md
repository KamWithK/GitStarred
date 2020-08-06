# GitStarred
GitHub isn't just a place to host code, but a social platform which brings people together!
With tens of millions of repositories, it's essential to find out how we can create popular, thriving open-source communities.
What better way to do this than analysing data?
In this project, we aim to decipher what differentiates beloved repositories from dead/dying ones (based on star count).

The project is split up into multiple parts:
* Data collection using the GitHub GraphQL API
* Data pipeline for feature engineering (finding, selecting and creating features)
* Modelling using H2O's AutoML model trainer (iteratively on small subsets of the raw ~50 GB dataset)
* Training the final/full model (and evaluating it)
* Creating a website to predict a repositories performance and provide suggestions

*Note that this project is currently a work in progress, so not everything here has been implemented yet.*
*Scroll down for setup instructions!*

## Key Problems
### Data Collection
To be capable of predicting how well a repository will perform there are several issues which need to be addressed.
The first is that the GitHub API imposes several large user restrictions.
In particular though:
* One user can only make 2000 queries each hour
* Only the first 10 pages (1000) results from a query may be collected
* Slow synchronous query speed

We can overcome the first limitation simply by allowing our code to run for longer (months?) or by using multiple tokens (ask your friends).
To get around the second problem though, we need to break down the search space into several "short periods" to query separately.
Since we know roughly how large a portion of repositories we can collect at once, and the broad period from which we want to gather our data, we can continuously (recursively) keep breaking down the search queries (by date periods) until all subsections are small enough.
Running data collection code asynchronously with a highly optimised PostgreSQL database greatly helps with speed!

*Note that the data collection code despite a large number of incremental improvements and often complete rewrites still is not perfect.*
*Towards the end, the collection process may slow down to a halt and so need to be manually restarted.*

### Data Processing/Feature Engineering
After collecting all our data there are two potential considerations/problems:
* Large dataset (~50 GB)
* Mixture of structured (numeric along with categorical values) and unstructured data (text)

The high (computational and so monetary) cost of training a model on our full dataset gives us no room for future improvement.
On the other hand, we can iteratively processes and train over smaller random subsets of data relatively quickly and easily.
This involves finding particularly impactful features (to keep), redundant/useless ones to discard and identify other potential improvements down the line (like finding a new feature).
This is easiest with basic structured features (numbers and categories), and harder with unstructured text (names, descriptions, topics and readme's).


First off we analyse our popularity metrics (stars, forks and watches).
Although we could use any metric, stars seem the most direct and so intuitive (so the rest can be ignored).
To easily interpret the results, min-max feature scaling is used.
This means the least popular repositories will have a value of zero and the most *amazing performers* 1!
This SQL query `SELECT Stars/$1 FROM Repositories` where `$1` is the maximum number of repositories (found with `SELECT max(Stars) FROM Repositories`) is an efficient way to extract it.


*Note that model performances along with feature importance graphs will indicate where to go next...*


Once we're confident we've gotten the most out of all numerical and categorical features, we'll use a mixture of manual feature extraction and natural language processing (NLP) techniques on our text.
Manual feature extraction (like word counts) provide quick and easy to use stats but require custom coding.
They provide a starting point but are less thorough than the slower (but potentially more concrete) NLP techniques.
Since the large dataset size stems from the readme's, we can reduce the amount of necessary processing to be done by purely analysing their titles.

For manual feature extraction:
* Number of words
* Number of emojis
* Whether titles use title case

Then later on for NLP (for readme's only with titles, to reduce the amount of present data):
* Lower casing
* Tokenisation
* Frequent/rare/stop word removal
* Lematisation
* Bigrams
* Bag of Words/Term Frequency Inverse Document Frequency

### Modelling
A random selection of ensemble models (including random forests and gradient boosted trees) can be trained on our processed data.
These work well with structured data (numbers, categories...), allowing us to start modelling quickly.

We can quickly train small models, and once we're satisfied to move onto retraining on the full dataset (for better results and generalisability)!
*Despite the previous processing work, training a model likely still requires a powerful computer.*
*Google Cloud Platform or Amazon Web Services can provide these!*

## Architecture
Since this isn't a *small project* (there's ~50 GB of data), speed and reliability are large issues.
To overcome this, we use the following open source technologies, frameworks and libraries:
* Asynchronous data collection using AsyncIO, AIOHTTP (for making requests to the GitHub API) and AsyncPG (for using a relational PostgreSQL database for reliable storage)
* Data pipeline with Apache Spark (instead of Pandas/Numpy because of its scalability) which interfaces with the previous PostgreSQL database
* NLP processing with SparkNLP (so it fits into the rest of the pipeline)
* Modelling with H2O AI (one library for a variety of ensemble models, including XGBoost itself) which work with Apache Spark through their Sparkling Water libraries

## Install Instructions
1. Install all system dependencies (Git, PostgreSQL, Anaconda)
2. Clone the repository through executing `git clone https://github.com/KamWithK/GitStarred/`
3. Ensure your user has access to PostgreSQL (through `sudo -u postgres createuser --interactive --pwprompt user_name`)
4. Create a PostgreSQL database (through `sudo -u postgres createdb database_name`)
5. Install Python packages (run `conda env update --prune -f environment.yml`)
6. Update the config (`Data/Config.json`) file
7. Run data collection code (if a dataset is provided skip to next step) which may need to be manually restarted towards the end
8. Perform feature engineering (feature selection and creation, optional but necessary to understand the data and therefore to improve the models)
9. Train the final model (using a finalised preprocessing pipeline)

*Currently spark plays nicely with Java 8, but not 11. So the `environment.yml` specifically installs version `8.0.152`.*

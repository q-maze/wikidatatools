# To create the dataset for the pipeline
from pykeen.triples import TriplesFactory
from pykeen.datasets import EagerDataset
import os

# To set a constant split
random_state = 3145
# Path to the data. Change for a different dataset.
# For this example we are loading the countries tsv file
datapath = '../example_subsets/countries.tsv'

# TripleFactory is PyKeens base data format for model building
tf = TriplesFactory.from_path(datapath)
# 80% training, 10% testing, and 10% validation split
training, testing, validation = tf.split([.8, .1, .1], random_state=random_state)

# Create the EagerDatset creation
# It will load all the data at once
dataset = EagerDataset(training, testing, validation)

# Save the EagerDataSet for the pipeline
ds_path = os.getcwd()+'/human_EA'
dataset.to_directory_binary(ds_path)

# To load the data, below for reference
dataset = EagerDataset.from_directory_binary(ds_path)
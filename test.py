import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import findspark
findspark.init()
from pyspark.sql import SparkSession

filename ='/Users/mikemoore26/Downloads/archive (36)/yelp_academic_dataset_tip.json'
#spark = SparkSession.builder.master('local').getOrCreate()
#df = spark.read.json(filename)



def json_csv(line : str) -> beam.pvalue.PCollection:
  import json
  import csv
  import pandas as pd

  line = json.loads(line)
  series = pd.Series(line)



  return series

def run():

  with beam.Pipeline() as pipeline:
  # Options
    print(type(pipeline))
    lines = pipeline | 'reading' >> beam.io.ReadFromText(filename) \
            | 'convert method' >> beam.Map(json_csv) \
            | beam.Map(print)

    # print(lines)
if __name__ == '__main__':
  run()
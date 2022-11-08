import argparse
import logging
import json
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.dataframe.convert import to_dataframe

# import findspark
# findspark.init()
# from pyspark.sql import SparkSession

# filename ='/Users/mikemoore26/Downloads/archive (36)/yelp_academic_dataset_tip.json'
#spark = SparkSession.builder.master('local').getOrCreate()
#df = spark.read.json(filename)


'''
python -m pipeline \
    --region us-west2  \
    --input gs://yelp_bucket-mm/yelp_academic_dataset_checkin.json \
    --output gs://yelp_bucket-mm/results/checkin \
    --runner DataflowRunner \
    --project algebraic-craft-367518 \
    --temp_location gs://yelp_bucket-mm/tmp/
'''


class Json_Csv(beam.DoFn):
    def process(self, line):
        line = json.loads(line)
        data = []
        for val in line.values():
            data.append(val)

        data = '|'.join(data)
        yield data 

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://yelp_bucket-mm',
      help='Input file to process.')

  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline() as pipeline:

    pipeline | 'reading' >> beam.io.ReadFromText(known_args.input) \
          | 'transform' >> beam.ParDo(Json_Csv() ) \
          | 'Write' >> beam.io.WriteToText(known_args.output)

#
if __name__ == '__main__':
  run()
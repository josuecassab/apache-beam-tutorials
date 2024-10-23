import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.kafka import ReadFromKafka
import json


def main():

    with open('consumer_config.json') as f:
        config = json.load(f)

    beam_options = PipelineOptions()

    with beam.Pipeline(options=beam_options) as p:

        msgs = p | ReadFromKafka(config,
                                 ['topic_0'],
                                 max_num_records= 2,
                                 start_read_time=0
                                 )
        decoded = msgs | beam.Map(lambda x: x[1])
        transformed = ( decoded | "decode" >> beam.Map(lambda x: x.decode('utf-8'))
                                | "convert into dict" >> beam.Map(lambda x: json.loads(x))
                                | beam.Map(lambda element: beam.window.TimestampedValue(element, element['timestamp']))
                               )
        transformed | "print2" >> beam.Map(print)


if __name__ == '__main__':

    main()
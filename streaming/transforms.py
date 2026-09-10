"""Explicit Spark schema and validation projection."""
from pyspark.sql import functions as F, types as T
from streaming.rules import METADATA_FIELDS, validate


def validated_frame(frame, rules):
    metadata = T.StructType([T.StructField(name, T.LongType() if name == 'source_row' else T.StringType())
                             for name in METADATA_FIELDS])
    payload = T.StructType([T.StructField(name, T.StringType()) for name in rules['input_fields']])
    schema = T.StructType([
        T.StructField('metadata', metadata), T.StructField('payload', payload),
        T.StructField('event_time', T.TimestampType()),
        T.StructField('quality_flags', T.ArrayType(T.StringType())),
        T.StructField('reasons', T.ArrayType(T.StringType())),
        T.StructField('details', T.ArrayType(T.StringType())),
    ])
    parse = F.udf(lambda raw: validate(raw, rules), schema)
    parsed = frame.withColumn('parsed', parse(F.col('value')))
    # Keep raw bytes for rejected and valid records, even fields excluded by the allowlist.
    return parsed.select('topic', 'partition', 'offset', 'value', 'timestamp',
                         'parsed.metadata', 'parsed.event_time', 'parsed.quality_flags',
                         'parsed.reasons', 'parsed.details',
                         F.struct(*[F.col('parsed.payload').getField(name).alias(name)
                                    for name in rules['allowlist']]).alias('payload'))

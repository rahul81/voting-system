# import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T


spark = (SparkSession
         .builder
         .appName('streaming-votes')
         .config("spark.sql.adaptive.enabled", "false")
         .getOrCreate())

spark.sparkContext.setLogLevel('ERROR')

candidates_df = spark.read.csv('/Users/rahuldas/mySpace/w2023/03-projects/voting-system/data/candidates.csv', header=True, inferSchema=True)

schema = T.StructType([
    T.StructField('id', T.StringType(), False),
    T.StructField('title', T.StringType(), False),
    T.StructField('firstName', T.StringType(), False),
    T.StructField('lastName', T.StringType(), False),
    T.StructField('gender', T.StringType(), False),
    T.StructField('age',T.IntegerType(), False),
    T.StructField('city',T.StringType(), False),
    T.StructField('state', T.StringType(), False),
    T.StructField('country', T.StringType(), False),
    T.StructField('postal_code', T.StringType(), False),
    T.StructField('voter_photo_url', T.StringType(), False),
    T.StructField('voted_to', T.StringType(), False),
    T.StructField('vote_timestamp', T.TimestampType(), False)
])

votes_stream_df = (spark.readStream
                   .format('kafka')
                   .option('kafka.bootstrap.servers','localhost:9092')
                   .option('subscribe', 'votes-topic')
                   .option("startingOffsets", "earliest")
                   .load()
                   .select(F.col('value').cast(T.StringType()))
                   .select(F.from_json(F.col('value'), schema).alias('data'))
                   .select('data.*'))


votes_stream_df = votes_stream_df.withWatermark('vote_timestamp', '1 minute')


voters_joined_df = votes_stream_df.join(candidates_df, votes_stream_df.voted_to==candidates_df.candidate_id, how='left_outer' )

print(voters_joined_df.printSchema())


votes_per_candidates = voters_joined_df.groupby('candidate_id','candidate_name','party').count().alias('total_votes')


(votes_per_candidates
 .writeStream
 .outputMode('complete')
 .format('console')
 .start()
 .awaitTermination())




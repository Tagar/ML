
## (skipping some global variables stuff)

## this script has to be spark-submitted with h2o-genmodel.jar passed through --jars parameter 

def score_partition_records (iter):
    
    # exec("genmodel = spark._jvm.hex.genmodel")    # this reference will fail.. 
    
    mojo_loader = genmodel.MojoModel.load(model_zip)

    model_wrapper = genmodel.easy.EasyPredictModelWrapper(mojo_loader)

    for record in iter:

        record_lines = record.split('\n')
    
        mcode = record_lines[0]   # 1st column - this would be matchcode
        response = None

        row = genmodel.easy.RowData()

        #feed into the `row` all the variables
        for kv in record_lines[1:-1]:      # ignore first line (matchcode) and last one (will be empty)
            k,v = kv.split('|')
            row.put(k,v)
    
            if k=='RESPONSE': response=v

        #now we're ready to score this one record
        prediction = model_wrapper.predictBinomial(row)

        probability = prediction.classProbabilities[1]        # assuming classProbabilities[1] is prediction for a "1"/response=yes

        #output match code, actual response, predicted probability of being a responder
        yield (mcode, int(response), float(probability))



from pyspark.sql.types import *

schema = StructType([ StructField('mcode'   , StringType(),  False)
                    , StructField('response', IntegerType(), False)
                    , StructField('prob1'   , DoubleType(),  False)
                     ])

scored_rdd = var_table_df.rdd.mapPartitions(score_partition_records)

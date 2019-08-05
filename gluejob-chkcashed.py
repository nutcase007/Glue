import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp ,input_file_name


print "===========================================================================================" 
print "===========================================================================================" 

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
args = getResolvedOptions(sys.argv, ['input_file_path'])
print "The input file path is -: ", args['input_file_path'] 
print "The TempDir is -: ", args['TempDir'] 
input_file = args['input_file_path'] 

sc = SparkContext()
glueContext = GlueContext(SparkContext.getOrCreate())
#glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "redshift-intg", table_name = "checks", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "redshift-intg", table_name = "checks", transformation_ctx = "datasource0")

dyF = glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': [ args['input_file_path'] ]},
        'csv',
        {'withHeader': True})

print "Full record count:  ", dyF.count()
dyF.printSchema()

datasource1 = dyF.toDF().withColumn("load_file", input_file_name()).withColumn("load_datetime", current_timestamp())

print(type(dyF))

#datasource1.describe().show()

datasource2 = dyF.fromDF(datasource1, glueContext, "datasource2")
print " After GlueContext Show  " 
print(type(datasource2))

datasource2.printSchema()

datasource4 = Filter.apply(frame = datasource2, f = lambda x: x["load_file"] == input_file)
print "input_file_path from AWS Lambda -:" , input_file

#print "Filtered records count -: ", datasource4.count()
print "Filtered records count - : ", Filter.apply(frame = datasource4, f = lambda x: x["load_file"] == input_file).count()

## @type: ApplyMapping
## @args: [mapping = [("account", "string", "acct", "string"), ("check_amount", "string", "chk_amt", "decimal(20,2)"), ("account_status", "string", "acct_stts", "string"), ("transaction_date", "string", "tran_dt", "date"), ("check_number", "string", "chk_num", "string"), ("payee", "string", "payee", "string"), ("check_cashed_date", "string", "chk_cashed_dt", "date")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource4, mappings = [("load_file", "string", "load_file", "string"),("load_datetime", "timestamp", "load_datetime", "timestamp"), ("account", "string", "acct", "string"), ("check_amount", "string", "chk_amt", "decimal(20,2)"), ("account_status", "string", "acct_stts", "string"), ("transaction_date", "string", "tran_dt", "date"), ("check_number", "string", "chk_num", "string"), ("payee", "string", "payee", "string"), ("check_cashed_date", "string", "chk_cashed_dt", "date")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["payee", "load_datetime", "tran_dt", "acct_stts", "chk_num", "load_file", "acct", "chk_amt", "chk_cashed_dt"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["payee", "load_datetime", "tran_dt", "acct_stts", "chk_num", "load_file", "acct", "chk_amt", "chk_cashed_dt"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_payment_hist_kyriba_chkcashed", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_payment_hist_kyriba_chkcashed", transformation_ctx = "resolvechoice3")

## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

## @type: DropNullFields
## @args: [transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
resolvechoice5 = DropNullFields.apply(frame = resolvechoice4, transformation_ctx = "resolvechoice5")
print(type(resolvechoice5))
resolvechoice5.printSchema()

## @type: DataSink
## @args: [database = "redshift-intg", table_name = "intg_payment_hist_kyriba_chkcashed", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice5, database = "redshift-intg", table_name = "intg_payment_hist_kyriba_chkcashed", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()


print "===========================================================================================" 
print "===========================================================================================" 
print " File movement is started : " 

import boto3

bucketname = "viac-intg-vdw-collection" 
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)
source = "kyriba/checks" 
target = "kyriba/processed"
source_filename = ''


for obj in my_bucket.objects.filter(Prefix=source):
    source_filename = (obj.key).split('/')[-1]
    copy_source = {
        'Bucket': bucketname,
        'Key': obj.key
    }
    target_filename = "{}/{}".format(target, source_filename)
    if source_filename in input_file : 
        print "Moving the input file - : ", input_file
        print "Source  File Name - : ", source_filename
        print "copy_source File Name - : ", copy_source
        print "bucketname File Name - : ", bucketname
        s3.meta.client.copy(copy_source, bucketname, target_filename)
        print "Target Folder is - S3://viac-intg-vdw-collection/kyriba/processed/ "
        s3.Object(bucketname,obj.key).delete()
        print "File is moved successfully at - : ", target_filename
    
   
print "===========================================================================================" 
print "===========================================================================================" 
  
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
## @args: [database = "redshift-intg", table_name = "payment", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "redshift-intg", table_name = "payment", transformation_ctx = "datasource0")

dyF = glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': [ args['input_file_path'] ]},
        'csv',
        {'withHeader': True})

print "Full record count:  ", dyF.count()
dyF.printSchema()


datasource1 = dyF.toDF().withColumn("load_file", input_file_name()).withColumn("load_dttime", current_timestamp())
print(type(dyF))

#datasource1.describe().show()
datasource2 = dyF.fromDF(datasource1, glueContext, "datasource2")
print " After GlueContext Show  " 
print(type(datasource2))

datasource2.printSchema()

datasource4 = Filter.apply(frame = datasource2, f = lambda x: x["load_file"] == input_file)
print "input_file_path from AWS Lambda -: " , input_file

#print "Filtered records count -: ", datasource4.count()
print "Filtered records count - : ", Filter.apply(frame = datasource4, f = lambda x: x["load_file"] == input_file).count()

## @type: ApplyMapping
## @args: [mapping = [("trans cd", "string", "trans_cd", "string"), ("src system", "string", "src_sys", "string"), ("payee bnk acct", "long", "payee_bnk_acct", "string"), ("account id", "string", "acct_id", "string"), ("pymnt method", "string", "pymnt_method", "string"), ("operating unit", "long", "oprtg_unit_id", "string"), ("pymnt id ref", "string", "pymnt_id_ref", "string"), ("request amt", "string", "req_amt", "decimal(18,0)"), ("invoice id", "string", "inv_id", "string"), ("payee aba", "string", "payee_aba", "string"), ("cycle date", "string", "cycle_dt", "date"), ("ssn", "long", "ssn", "string"), ("src company", "string", "src_co", "string"), ("src trans type", "string", "src_trans_typ", "string"), ("reason type", "string", "rsn_typ", "string"), ("name1", "string", "name1", "string"), ("name2", "string", "name2", "string"), ("address1", "string", "addr1", "string"), ("address2", "string", "addr2", "string"), ("address3", "string", "addr3", "string"), ("address4", "string", "addr4", "string"), ("city", "string", "city", "string"), ("country", "string", "cntry", "string"), ("state", "string", "state", "string"), ("postal", "long", "postal", "string"), ("erisa indicator", "string", "erisa_ind", "string"), ("account", "long", "acct", "int"), ("dept id", "string", "dept_id", "string"), ("product", "string", "wrktg_prod_id", "string"), ("user id", "string", "user_id", "string"), ("speed chart code", "string", "speed_chart_cd", "string"), ("gl business unit", "string", "gl_bus_unit", "string"), ("eds business unit", "string", "eds_bus_unit", "string"), ("holder id", "string", "holder_id", "string"), ("unclaimed property type code", "string", "unclmd_prpty_typ_cd", "string"), ("request type code", "string", "req_typ_cd", "string"), ("check cashed date", "string", "chk_cashed_dt", "date"), ("workday id", "string", "workday_id", "string"), ("message 1", "string", "msg_1", "string"), ("message 2", "string", "msg_2", "string"), ("cancel date", "string", "cxl_dt", "date"), ("check date", "string", "chk_dt", "date")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource4, mappings = [("load_file", "string", "load_file", "string"),("load_dttime", "timestamp", "load_dttime", "timestamp"),("trans_cd", "string", "trans_cd", "string"), ("src_system", "string", "src_sys", "string"), ("payee_bnk_acct", "string", "payee_bnk_acct", "string"), ("account_id", "string", "acct_id", "string"), ("pymnt_method", "string", "pymnt_method", "string"), ("operating_unit", "string", "wrktg_oprtg_unit_id", "string"), ("pymnt_id_ref", "string", "pymnt_id_ref", "string"), ("request_amt", "string", "req_amt", "decimal(18,2)"), ("invoice_id", "string", "inv_id", "string"), ("payee_aba", "string", "payee_aba", "string"), ("cycle_date", "string", "cycle_dt", "date"), ("ssn", "string", "ssn", "string"), ("src_company", "string", "src_co", "string"), ("src_trans_type", "string", "src_trans_typ", "string"), ("reason_type", "string", "rsn_typ", "string"), ("name1", "string", "name1", "string"), ("name2", "string", "name2", "string"), ("address1", "string", "addr1", "string"), ("address2", "string", "addr2", "string"), ("address3", "string", "addr3", "string"), ("address4", "string", "addr4", "string"), ("city", "string", "city", "string"), ("country", "string", "cntry", "string"), ("state", "string", "state", "string"), ("postal", "string", "postal", "string"), ("erisa_indicator", "string", "erisa_ind", "string"), ("account", "string", "acct", "string"), ("dept_id", "string", "dept_id", "string"), ("product", "string", "wrktg_prod_id", "string"), ("user_id", "string", "user_id", "string"), ("speed_chart_code", "string", "speed_chart_cd", "string"), ("gl_business_unit", "string", "gl_bus_unit", "string"), ("eds_business_unit", "string", "eds_bus_unit", "string"), ("holder_id", "string", "holder_id", "string"), ("unclaimed_property_type_code", "string", "unclmd_prpty_typ_cd", "string"), ("request_type_code", "string", "req_typ_cd", "string"), ("check_cashed_date", "string", "chk_cashed_dt", "date"), ("workday_id", "string", "workday_id", "string"), ("message_1", "string", "msg_1", "string"), ("message_2", "string", "msg_2", "string"), ("cancel_date", "string", "cxl_dt", "date"), ("check_date", "string", "chk_dt", "date")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["trans_cd", "payee_aba", "msg_2", "gl_bus_unit", "cxl_dt", "city", "msg_1", "pymnt_method", "req_typ_cd", "ssn", "acct_id", "chk_dt", "pymnt_id_ref", "src_co", "cntry", "payee_bnk_acct", "inv_id", "state", "chk_cashed_dt", "unclmd_prpty_typ_cd", "req_amt", "wrktg_prod_id", "speed_chart_cd", "addr2", "eds_bus_unit", "addr1", "addr4", "src_sys", "rsn_typ", "addr3", "workday_id", "oprtg_unit_id", "load_dttime", "user_id", "holder_id", "load_file", "postal", "name2", "dept_id", "src_trans_typ", "name1", "cycle_dt", "acct", "erisa_ind"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["trans_cd", "payee_aba", "msg_2", "gl_bus_unit", "cxl_dt", "city", "msg_1", "pymnt_method", "req_typ_cd", "ssn", "acct_id", "chk_dt", "pymnt_id_ref", "src_co", "cntry", "payee_bnk_acct", "inv_id", "state", "chk_cashed_dt", "unclmd_prpty_typ_cd", "req_amt", "wrktg_prod_id", "speed_chart_cd", "addr2", "eds_bus_unit", "addr1", "addr4", "src_sys", "rsn_typ", "addr3", "workday_id", "wrktg_oprtg_unit_id", "load_dttime", "user_id", "holder_id", "load_file", "postal", "name2", "dept_id", "src_trans_typ", "name1", "cycle_dt", "acct", "erisa_ind"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_payment_hist_kyriba_pmt", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_payment_hist_kyriba_pmt", transformation_ctx = "resolvechoice3")
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
## @args: [database = "redshift-intg", table_name = "intg_payment_hist_kyriba_pmt", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice5, database = "redshift-intg", table_name = "intg_payment_hist_kyriba_pmt", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()


print "===========================================================================================" 
print "===========================================================================================" 
print " File movement is started : " 

import boto3

bucketname = "viac-intg-vdw-collection" 
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)
source = "kyriba/payments"
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
        print "Moving the input file -: ", input_file 
        print "Source  File Name - : ", source_filename
        print "copy_source File Name - : ", copy_source
        print "bucketname File Name - : ", bucketname
        s3.meta.client.copy(copy_source, bucketname, target_filename)
        print "Target Folder is - s3://viac-intg-vdw-collection/kyriba/processed/ "
        s3.Object(bucketname,obj.key).delete()
        print "File is moved successfully at -: ", target_filename
    
   
print "===========================================================================================" 
print "===========================================================================================" 
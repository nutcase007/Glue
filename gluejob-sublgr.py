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
## @args: [database = "redshift-intg", table_name = "vdw", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "redshift-intg", table_name = "vdw", transformation_ctx = "datasource0")


dyF = glueContext.create_dynamic_frame.from_options(
        's3',
        {'paths': [ args['input_file_path'] ]},
        'csv',
        {'withHeader': True})

print "Full record count:  ", dyF.count()
dyF.printSchema()


print "again, The input file path is -: ", input_file_name()
datasource1 = dyF.toDF().withColumn("load_file", input_file_name()).withColumn("load_datetime", current_timestamp())

print(type(dyF))

#datasource1.describe().show()
datasource2 = dyF.fromDF(datasource1, glueContext, "datasource2")


print " After GlueContext Show  " 
print(type(datasource2))
datasource2.printSchema()

datasource4 = Filter.apply(frame = datasource2, f = lambda x: x["load_file"] == input_file)
print "input_file_path from AWS Lambda -:" , input_file
#print "Filtered records count: ", datasource4.count()
print "Filtered records count -: ", Filter.apply(frame = datasource4, f = lambda x: x["load_file"] == input_file).count()


## @type: ApplyMapping
## @args: [mapping = [("journalkey", "long", "jrnl_key", "long"), ("ledgeraccountreferenceidtype", "string", "lgr_acct_refid_typ", "string"), ("admin_trans_code", "long", "admin_trans_cd", "string"), ("currency", "string", "curr", "string"), ("ledgertype", "string", "lgr_typ", "string"), ("bookcode", "string", "book_cd", "string"), ("companyreferenceidtype", "string", "corefid_typ", "string"), ("journalsource", "string", "jrnl_scr", "string"), ("journalentrymemo", "string", "jrnl_entry_memo", "string"), ("createreversal", "long", "crtrvsl", "string"), ("worktag_product_id", "long", "wrktg_prod_id", "string"), ("linecompanyreferenceidtype", "string", "ln_corefid_typ", "string"), ("linecompanyreferenceid", "string", "ln_coref_id", "string"), ("worktag_region_reference_id", "string", "wrktg_rgn_id", "string"), ("ledgeraccountreferenceid_parentid", "string", "lgr_acct_refid_parid", "string"), ("ledgeraccountreferenceid_parentidtype", "string", "lgr_acct_refid_parid_typ", "string"), ("ledgeraccountreferenceid", "long", "lgr_acct_refid", "int"), ("accountingdate", "string", "acctg_dt", "date"), ("debitamount", "double", "lgr_dr_amt", "decimal(20,6)"), ("linememo", "string", "ln_memo", "string"), ("journallineexternalreferenceid", "long", "jrnl_ln_extref_id", "long"), ("creditamount", "double", "lgr_cr_amt", "decimal(20,6)"), ("worktag_operating_unit_id", "long", "wrktg_oprtg_unit_id", "string"), ("worktag_fund_id", "string", "wrktg_fund_id", "string"), ("worktag_bank_account_id", "string", "wrktg_bank_acct_id", "string"), ("worktag_spend_category_id", "string", "wrktg_spnd_cat_id", "string"), ("journal_line_reference", "string", "jrnl_ln_ref", "string"), ("journal_line_description", "string", "jrnl_ln_descr", "string"), ("policy_number", "string", "plcy_num", "string"), ("spc_product", "string", "spc_prod", "string"), ("user_id", "string", "user_id", "string"), ("line_descr_2", "string", "ln_descr_2", "string"), ("line_descr_3", "string", "ln_descr_3", "string"), ("broker_dealer", "string", "broker_dlr", "string"), ("issue_date", "string", "issue_dt", "date"), ("reversaldate", "string", "rvsl_dt", "date"), ("companyreferenceid", "string", "coref_id", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
print "ApplyMapping.apply is started -: applymapping1  - ",input_file

applymapping1 = ApplyMapping.apply(frame = datasource4, mappings = [("load_file", "long", "load_file", "long"),("load_datetime", "timestamp", "load_datetime", "timestamp"),("journalkey", "string", "jrnl_key", "bigint"), ("ledgeraccountreferenceidtype", "string", "lgr_acct_refid_typ", "string"), ("admin_trans_code", "string", "admin_trans_cd", "string"), ("currency", "string", "curr", "string"), ("ledgertype", "string", "lgr_typ", "string"), ("bookcode", "string", "book_cd", "string"), ("companyreferenceidtype", "string", "corefid_typ", "string"), ("journalsource", "string", "jrnl_scr", "string"), ("journalentrymemo", "string", "jrnl_entry_memo", "string"), ("createreversal", "string", "crtrvsl", "string"), ("worktag_product_id", "string", "wrktg_prod_id", "string"), ("linecompanyreferenceidtype", "string", "ln_corefid_typ", "string"), ("linecompanyreferenceid", "string", "ln_coref_id", "string"), ("worktag_region_reference_id", "string", "wrktg_rgn_id", "string"), ("ledgeraccountreferenceid_parentid", "string", "lgr_acct_refid_parid", "string"), ("ledgeraccountreferenceid_parentidtype", "string", "lgr_acct_refid_parid_typ", "string"), ("ledgeraccountreferenceid", "string", "lgr_acct_refid", "string"), ("accountingdate", "string", "acctg_dt", "date"), ("debitamount", "string", "lgr_dr_amt", "decimal(20,6)"), ("linememo", "string", "ln_memo", "string"), ("journallineexternalreferenceid", "string", "jrnl_ln_extref_id", "bigint"), ("creditamount", "string", "lgr_cr_amt", "decimal(20,6)"), ("worktag_operating_unit_id", "string", "wrktg_oprtg_unit_id", "string"), ("worktag_fund_id", "string", "wrktg_fund_id", "string"), ("worktag_bank_account_id", "string", "wrktg_bank_acct_id", "string"), ("worktag_spend_category_id", "string", "wrktg_spnd_cat_id", "string"), ("journal_line_reference", "string", "jrnl_ln_ref", "string"), ("journal_line_description", "string", "jrnl_ln_descr", "string"), ("policy_number", "string", "plcy_num", "string"), ("spc_product", "string", "spc_prod", "string"), ("user_id", "string", "user_id", "string"), ("line_descr_2", "string", "ln_descr_2", "string"), ("line_descr_3", "string", "ln_descr_3", "string"), ("broker_dealer", "string", "broker_dlr", "string"), ("issue_date", "string", "issue_dt", "date"), ("reversaldate", "string", "rvsl_dt", "date"), ("companyreferenceid", "string", "coref_id", "string")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["load_datetime", "lgr_acct_refid_typ", "broker_dlr", "ln_descr_3", "ln_descr_2", "wrktg_bank_acct_id", "wrktg_rgn_id", "ln_coref_id", "spc_prod", "jrnl_entry_memo", "plcy_num", "issue_dt", "ln_memo", "lgr_acct_refid_parid_typ", "rvsl_dt", "coref_id", "curr", "wrktg_spnd_cat_id", "wrktg_prod_id", "wrktg_fund_id", "corefid_typ", "jrnl_ln_extref_id", "acctg_dt", "jrnl_ln_descr", "crtrvsl", "ln_corefid_typ", "lgr_acct_refid", "jrnl_ln_ref", "book_cd", "lgr_dr_amt", "jrnl_key", "lgr_acct_refid_parid", "intrl_sublgrentry_id", "lgr_typ", "user_id", "load_file", "wrktg_oprtg_unit_id", "lgr_cr_amt", "admin_trans_cd", "jrnl_scr"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["load_datetime", "lgr_acct_refid_typ", "broker_dlr", "ln_descr_3", "ln_descr_2", "wrktg_bank_acct_id", "wrktg_rgn_id", "ln_coref_id", "spc_prod", "jrnl_entry_memo", "plcy_num", "issue_dt", "ln_memo", "lgr_acct_refid_parid_typ", "rvsl_dt", "coref_id", "curr", "wrktg_spnd_cat_id", "wrktg_prod_id", "wrktg_fund_id", "corefid_typ", "jrnl_ln_extref_id", "acctg_dt", "jrnl_ln_descr", "crtrvsl", "ln_corefid_typ", "lgr_acct_refid", "jrnl_ln_ref", "book_cd", "lgr_dr_amt", "jrnl_key", "lgr_acct_refid_parid", "intrl_sublgrentry_id", "lgr_typ", "user_id", "load_file", "wrktg_oprtg_unit_id", "lgr_cr_amt", "admin_trans_cd", "jrnl_scr"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_sub_ledger_hist_gl_sublgrentries", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
print "ResolveChoice make_cols is started (selectfields2) -: ", args['TempDir'] 
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "redshift-intg", table_name = "intg_sub_ledger_hist_gl_sublgrentries", transformation_ctx = "resolvechoice3")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
print "ResolveChoice make_cols is started (resolvechoice3) -: "
resolvechoice4 = ResolveChoice.apply(frame = resolvechoice3, choice = "make_cols", transformation_ctx = "resolvechoice4")

## @type: DropNullFields
## @args: [transformation_ctx = "<transformation_ctx>"]
## @return: <output>
## @inputs: [frame = <frame>]
print "The DropNullFields is started -: " 
resolvechoice5 = DropNullFields.apply(frame = resolvechoice4, transformation_ctx = "resolvechoice5")
print(type(resolvechoice5))
resolvechoice5.printSchema()

## @type: DataSink
## @args: [database = "redshift-intg", table_name = "intg_sub_ledger_hist_gl_sublgrentries", redshift_tmp_dir = TempDir, transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
print "The TempDir is -: ", args['TempDir'] 
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice5, database = "redshift-intg", table_name = "intg_sub_ledger_hist_gl_sublgrentries", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
print "After , the TempDir is -: ", args['TempDir'] 

job.commit()


print "===========================================================================================" 
print "===========================================================================================" 
print " File movement is started : " 

import boto3

bucketname = "viac-intg-glprep-distribution" 
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucketname)
source = "venerable/subledger/vdw"
target = "venerable/processed"
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
        print "Source  File Name -: ", source_filename
        print "copy_source File Name -: ", copy_source
        print "bucketname File Name -: ", bucketname
        s3.Object(bucketname,obj.key).delete()
        print "File (",source_filename,") is deleted successfully at -: ", copy_source
    
   
print "===========================================================================================" 
print "===========================================================================================" 
    
# USAGE: 
athena-auditor
 -a,--all                  Crawl all databases of a given catalog. If set,
                           'database' option is ignored
 -b,--bucketOutput <arg>   Athena result bucket output
 -c,--config <arg>         Configuration file location
 -d,--database <arg>       Glue metastore database name, 'database' and
                           'catalog' options are not set, then database
                           option is automaticaly set to 'default'
                           database.
 -o,--output <arg>         output path for this audit, default to
                           athena-auditor-<timestamp>.csv
 -p,--profile <arg>        Profile to use for credential provider, if not
                           set, InstanceProfileCredentialsProvider is used
 -r,--region <arg>         Region where request is done, if not set,
                           default to 'eu-west-3'

# Launch examples :
java -jar athena-auditor-fat-exec.jar -b foo -p bar -c configuration.properties
java -jar athena-auditor-fat-exec.jar -b foo -p bar -c /media/key/AuditSwamp/src/main/resources/configuration.properties
java -jar athena-auditor-fat-exec.jar -p dev@ADFS-User -a -o /media/key/auditS3swamp -b s3://s3-athena/test-audit/
java -jar athena-auditor-fat-exec.jar -p dev@ADFS-User -a -o /media/key/auditS3swamp/auditResult.csv -b s3://s3-athena/test-audit/
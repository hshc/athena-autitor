package fr.hshc.aws.audit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.Database;
import software.amazon.awssdk.services.athena.model.ListDatabasesResponse;
import software.amazon.awssdk.services.athena.model.ListTableMetadataResponse;

public class AthenaAuditor {

	public static String SEPARATOR = "\t";
	public static String VERSION_TABLE_TIMESTAMP_REGEX = "\\d{4}/\\d{2}/\\d{2}/\\d{4}-\\d{2}-\\d{2}t\\d{2}:\\d{2}:\\d{2}";
	
	private AthenaClientFactory clientFactory = null;
	private AthenaClient        client        = null;
	private int                 nbLignes      = 0;

	private final String        configPath;
	
	private String                 catalog      = null;
	private String                 database     = null;
	private String                 bucketOutput = null;
	private AwsCredentialsProvider profile      = null;
	private Region                 region       = null;
	private boolean                allDatabases = false;
	private Path                   outputPath   = null;
	private List<String>           whiteList    = null;
	private List<String>           blackList    = null;

	public AthenaAuditor(final CommandLine cmd) {
		String output     = null;
		this.configPath   = cmd.getOptionValue("config");
		this.allDatabases = cmd.hasOption("all");
		
		if (this.configPath != null) {
			Configurations configs = new Configurations();
			try {
			    Configuration config = configs.properties(this.configPath);
			    
			    SEPARATOR                     = config.containsKey("separator")                     ? config.getString("separator")                     : SEPARATOR;
			    VERSION_TABLE_TIMESTAMP_REGEX = config.containsKey("version_table_timestamp_regex") ? config.getString("version_table_timestamp_regex") : VERSION_TABLE_TIMESTAMP_REGEX;
			    
			    this.whiteList    = config.containsKey("whiteList")    ? config.getList(String.class, "whiteList") : null;
			    this.blackList    = config.containsKey("blackList")    ? config.getList(String.class, "blackList") : null;
			    this.catalog      = config.containsKey("catalog")      ? config.getString("catalog")               : null;
			    this.database     = config.containsKey("database")     ? config.getString("database")              : null;
			    this.bucketOutput = config.containsKey("bucketOutput") ? config.getString("bucketOutput")          : null;
			    this.profile      = config.containsKey("profile")      ? ProfileCredentialsProvider.create(config.getString("profile")): null;
			    this.region       = config.containsKey("region")       ? Region.of(config.getString("region"))     : null;
			    this.allDatabases = config.containsKey("all")          ? config.getBoolean("all")                  : this.allDatabases;
			    output            = config.containsKey("output")       ? config.getString("output")                : null;
			}
			catch (ConfigurationException cex){
			    cex.printStackTrace();
			}
		}
		this.catalog        = (this.catalog      != null ? this.catalog      : cmd.getOptionValue("catalog", "AwsDataCatalog"));
		this.database       = (this.database     != null ? this.database     : cmd.getOptionValue("database", "default"));
		this.bucketOutput   = (this.bucketOutput != null ? this.bucketOutput : cmd.getOptionValue("bucketOutput", "s3://s3-athena/test-audit/"));
		this.profile        = (this.profile      != null ? this.profile      : ProfileCredentialsProvider.create(cmd.getOptionValue("profile")));
		this.region         = (this.region       != null ? this.region       : Region.of(cmd.getOptionValue("region", "eu-west-3")));
		String defaultOutputFileName = "athena-auditor-" + (this.allDatabases?this.catalog:this.database) + "_" + new SimpleDateFormat("yyyyMMdd-HHmm").format(new Date()) + ".csv";
		output           = cmd.getOptionValue("output", defaultOutputFileName);
		
		System.out.println("-----------------------------------------------");
		System.out.println("-                Configuration                -");
		System.out.println("catalog      - \""+this.catalog+"\"            ");
		System.out.println("database     - \""+this.database+"\"           ");
		System.out.println("bucketOutput - \""+this.bucketOutput+"\" ");
		System.out.println("profile      - \""+this.profile+"\"            ");
		System.out.println("region       - \""+this.region+"\"             ");
		System.out.println("all          - \""+this.allDatabases+"\"       ");
		System.out.println("output       - \""+output+"\"                  ");
		System.out.println("-----------------------------------------------\n\n");
		
		File outputFile = new File(output);
		if (output.endsWith("/") || outputFile.exists() && outputFile.isDirectory()) {
			output = outputFile.getAbsolutePath()+"/" + defaultOutputFileName;
		}
		this.outputPath = Paths.get(output);
	}

	public AthenaClientFactory getClientFactory () {
		if (clientFactory == null) {
			clientFactory = new AthenaClientFactory(this.profile, this.region);
		}
		return clientFactory;
	}
	
	public AthenaClient getClient () {
		if (this.client == null) {
			this.client = this.getClientFactory().createClient();
		}
		return this.client;
	}
	
	public static void main(String[] args) {
		CommandLine cmd = null;
		try {
			cmd = parseCommand(args);
		} catch (ParseException e) {
            System.exit(1);
		}
		
		AthenaAuditor athenaAuditor = new AthenaAuditor(cmd);
		athenaAuditor.startAudit();
	}

	public void startAudit() {
		this.nbLignes = 0;
		try (BufferedWriter writer = Files.newBufferedWriter(this.outputPath, Charset.forName("UTF-8"))) {
			System.out.println("Writting to \""+this.outputPath.toAbsolutePath().toString()+"\"\n\n");
			String headers = "database" + SEPARATOR + headersSplitTableName() + SEPARATOR + headersMetaInfos() + SEPARATOR + headersHeadersInfo() + "\n";
			if (this.allDatabases) {
				System.out.print(headers);
				writer.write(headers);
				this.auditCatalog(writer);
			} else {
				System.out.print(headers);
				writer.write(headers);
				this.auditDataBase(this.database, writer);
			}
		} catch (IOException | RuntimeException ex) {
			ex.printStackTrace();
		}
		System.out.println(nbLignes);
		this.nbLignes = 0;
	}
	
	private void auditCatalog(final Writer pWriter) {
		this.getClient().listDatabasesPaginator((software.amazon.awssdk.services.athena.model.ListDatabasesRequest.Builder builder) -> {
			builder.catalogName(this.catalog);
		}).stream().forEach((ListDatabasesResponse databasesPage) -> {
			databasesPage.databaseList().stream().filter((Database db) -> {
//				System.out.println(db.name()+" in whitelist ? - " + (this.whiteList == null || this.whiteList.contains(db.name())));
				return this.whiteList == null || this.whiteList.contains(db.name());
			}).filter((Database db) -> {
//				System.out.println(db.name()+" in blacklist ? - " + (this.blackList == null || this.blackList.contains(db.name())));
				return this.blackList == null || !this.blackList.contains(db.name());
			}).forEach((Database db) -> {
//				System.out.println("auditDataBase("+db.name()+", pWriter);\n");
				auditDataBase(db.name(), pWriter);
			});
		});
	}
	
	private void auditDataBase(final String pDatabase, final Writer pWriter) {
		this.getClient().listTableMetadataPaginator((software.amazon.awssdk.services.athena.model.ListTableMetadataRequest.Builder builder) -> {
			builder.catalogName(this.catalog).databaseName(pDatabase);
		}).stream().forEach((ListTableMetadataResponse tablesPage) -> {
			tablesPage.tableMetadataList().stream().map(table -> {
				return new TestTableRequest(table, pDatabase, this.bucketOutput, this.getClient()).submit();
			})
			.map((TestTableResult t) -> t.waitForQueryToComplete())
			.map((TestTableResult t) -> t.processResultRows())
			.forEach((TestTableResult t) -> {
				String outline = pDatabase 
						+ SEPARATOR + t.splitTableName() 
						+ SEPARATOR + t.metaInfos() 
						+ SEPARATOR + t.headersInfos() + "\n";
				try {
					pWriter.write(outline);
					pWriter.flush();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				System.out.print(outline);
				this.nbLignes++;
			});
		});
	}
	
	public static String headersSplitTableName() {
		String out  = "origine"
		+ SEPARATOR + "domaine"
		+ SEPARATOR + "source"
		+ SEPARATOR + "table"
		+ SEPARATOR + "timestamp"
		+ SEPARATOR + "status"
		+ SEPARATOR + "erreur";
		return out;
	}
	public static String headersMetaInfos() {
		String output = "recordCount"
		  + SEPARATOR + "averageRecordSize"
		  + SEPARATOR + "CellsCount"
		  + SEPARATOR + "tableSize"
		  + SEPARATOR + "inputformat"
		  + SEPARATOR + "compressionType"
		  + SEPARATOR + "classification"
		  + SEPARATOR + "serde.serialization.lib"
		  + SEPARATOR + "outputformat"
		  + SEPARATOR + "location";
		return output;
	}
	public static String headersHeadersInfo() {
		String output = "columnNames" + SEPARATOR + "numColumns";
		return output;
	}
	
	private static CommandLine parseCommand(String[] args) throws ParseException {
		
		Option oConfiguration = new Option("c", "config", true, "Configuration file location");
		oConfiguration.setValueSeparator('=');
		oConfiguration.setRequired(false);
		
		Option oAthenaBucketOutput = new Option("b", "bucketOutput", true, "Athena result bucket output");
		oAthenaBucketOutput.setValueSeparator('=');
		oAthenaBucketOutput.setRequired(true);
		Option oProfile = new Option("p", "profile", true, "Profile to use for credential provider, if not set, InstanceProfileCredentialsProvider is used");
		oProfile.setValueSeparator('=');
		oProfile.setRequired(true);
		
		Option oCataolog = new Option("c", "catalog", true, "Athena catalog name, if database option is not set, crawl each database of this catalog");
		oCataolog.setValueSeparator('=');
		oCataolog.setRequired(false);
		Option oDatabase = new Option("d", "database", true, "Glue metastore database name, 'database' and 'catalog' options are not set, then database option is automaticaly set to 'default' database.");
		oDatabase.setValueSeparator('=');
		oDatabase.setRequired(false);
		Option oRegion = new Option("r", "region", true, "Region where request is done, if not set, default to 'eu-west-3'");
		oRegion.setValueSeparator('=');
		oRegion.setRequired(false);
		Option oAllDatabases = new Option("a", "all", false, "Crawl all databases of a given catalog. If set, 'database' option is ignored");
		oAllDatabases.setValueSeparator('=');
		oAllDatabases.setRequired(false);
		Option oOut = new Option("o", "output", true, "output path for this audit, default to athena-auditor-<timestamp>.csv");
		oOut.setValueSeparator('=');
		oOut.setRequired(false);
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        Options options = new Options()
        	.addOption(oCataolog)
        	.addOption(oDatabase)
        	.addOption(oAthenaBucketOutput)
        	.addOption(oProfile)
        	.addOption(oRegion)
        	.addOption(oAllDatabases)
        	.addOption(oOut)
        	.addOption(oConfiguration);
        
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("athena-auditor", options);
            throw e;
        }
		return cmd;
	}

}

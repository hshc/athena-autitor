package fr.hshc.aws.audit;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.ListTableMetadataRequest;
import software.amazon.awssdk.services.athena.model.QueryExecutionContext;
import software.amazon.awssdk.services.athena.model.ResultConfiguration;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.TableMetadata;

public class TestTableRequest {
	private final String outputBucket;
	private final String database;
	private final AthenaClient client;
	private final TableMetadata table;

	public TestTableRequest(final TableMetadata pTableMetadata, 
			final String pDatabaseName,
			final String pOutputBucket, 
			final AthenaClient pAthenaClient) {
		this.outputBucket = pOutputBucket;
		this.database = pDatabaseName;
		this.table = pTableMetadata;
		this.client = pAthenaClient;
	}

	public TestTableResult submit() {
		String queryResult = null;
		AthenaException exp = null;
		try {
			queryResult = this. submitAthenaQuery();
		} catch (AthenaException e) {
			exp = e;
		}
		return new TestTableResult(this.table, queryResult, exp, client);
	}

	public String submitAthenaQuery() {
		// The QueryExecutionContext allows us to set the database
		QueryExecutionContext queryExecutionContext = QueryExecutionContext.builder().database(this.database).build();

		// The result configuration specifies where the results of the query should go
		ResultConfiguration resultConfiguration = ResultConfiguration.builder().outputLocation(this.outputBucket)
				.build();

		StartQueryExecutionRequest startQueryExecutionRequest = StartQueryExecutionRequest.builder()
				.queryString("SELECT * FROM \"" + this.database + "\".\"" + this.table.name() + "\" limit 200;")
				.queryExecutionContext(queryExecutionContext).resultConfiguration(resultConfiguration).build();

		StartQueryExecutionResponse startQueryExecutionResponse = this.client
				.startQueryExecution(startQueryExecutionRequest);
		return startQueryExecutionResponse.queryExecutionId();
	}
	public static void main(String[] args) {

	}
}

package fr.hshc.aws.audit;
import static fr.hshc.aws.audit.AthenaAuditor.SEPARATOR;
import static fr.hshc.aws.audit.AthenaAuditor.VERSION_TABLE_TIMESTAMP_REGEX;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.AthenaException;
import software.amazon.awssdk.services.athena.model.Column;
import software.amazon.awssdk.services.athena.model.ColumnInfo;
import software.amazon.awssdk.services.athena.model.Datum;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionRequest;
import software.amazon.awssdk.services.athena.model.GetQueryExecutionResponse;
import software.amazon.awssdk.services.athena.model.GetQueryResultsRequest;
import software.amazon.awssdk.services.athena.model.QueryExecutionState;
import software.amazon.awssdk.services.athena.model.Row;
import software.amazon.awssdk.services.athena.model.TableMetadata;
import software.amazon.awssdk.services.athena.paginators.GetQueryResultsIterable;

public class TestTableResult {
	public static final long SLEEP_AMOUNT_IN_MS = 3 * 1000;
	
	private TableMetadata       table;
	private Exception           error;
	private AthenaClient        client;
	private String              queryResultId;
	private QueryExecutionState status = QueryExecutionState.UNKNOWN_TO_SDK_VERSION;
	
	private String              metaInfos = null;
	private String              headersInfos = null;

	public QueryExecutionState getStatus() {
		return status;
	}

	public TestTableResult(final TableMetadata table, final String queryResult, final Exception error, final AthenaClient client) {
		super();
		this.client = client;
		this.table = table;
		this.error = error;
		this.queryResultId = queryResult;
	}

	public boolean isKo() {
		return error != null;
	}

	public TableMetadata getTable() {
		return table;
	}

	public Exception getError() {
		return error;
	}


	// Wait for an Amazon Athena query to complete, fail or to be cancelled
	public TestTableResult waitForQueryToComplete() {
		if (this.isKo()) {
			return this;
		}
		GetQueryExecutionRequest getQueryExecutionRequest = GetQueryExecutionRequest.builder()
				.queryExecutionId(this.queryResultId).build();

		GetQueryExecutionResponse getQueryExecutionResponse;

		loop: while (true) {
			getQueryExecutionResponse = this.client.getQueryExecution(getQueryExecutionRequest);
			this.status = getQueryExecutionResponse.queryExecution().status().state();

			switch (this.status) {
			case FAILED:
				this.error = new RuntimeException(getQueryExecutionResponse.queryExecution().status().stateChangeReason());
				break loop;
			case CANCELLED:
				this.error = new RuntimeException("Amazon Athena query cancelled.");
				break loop;
			case SUCCEEDED:
				break loop;
			default:
				try {
					Thread.sleep(SLEEP_AMOUNT_IN_MS);
				} catch (InterruptedException e) {
					this.error = e;
				}
			}
		}
		return this;
	}

	// This code retrieves the results of a query
	public TestTableResult processResultRows() {
		if (this.isKo()) {
			return this;
		}
		try {
			// Max Results can be set but if its not set,
			// it will choose the maximum page size
			GetQueryResultsRequest getQueryResultsRequest = 
					GetQueryResultsRequest.builder()
					.queryExecutionId(this.queryResultId).build();

			GetQueryResultsIterable getQueryResultsResults = 
					this.client
					.getQueryResultsPaginator(getQueryResultsRequest);

//			for (GetQueryResultsResponse result : getQueryResultsResults) {
//				List<ColumnInfo> columnInfoList = result.resultSet().resultSetMetadata().columnInfo();
//				List<Row> results = result.resultSet().rows();
//				processRow(results, columnInfoList);
//			}

		} catch (AthenaException e) {
			this.error = e;
		}
		return this;
	}
	
//	private static void processRow(final List<Row> pRow, final List<ColumnInfo> pColumnInfoList) {
//		for (Row myRow : pRow) {
//			List<Datum> allData = myRow.data();
//			for (Datum data : allData) {
////				System.out.println("The value of the column is " + data.varCharValue());
//			}
//		}
//	}
	
	public String splitTableName() {
		int k = findTimestamp();
		String timeStamp = this.table.name().substring(k);
		String tableName = this.table.name().substring(0, "".equals(timeStamp)?k:k-1);
		String[] table = tableName.split("/");
		String tableSet = "";
		for (int i=4, j=table.length; i>0; i--,j--) {
			tableSet = (j>0?table[j-1]:"") + SEPARATOR + tableSet;
		}
		String outline = tableSet + 
				timeStamp + SEPARATOR + 
				this.status.name() + SEPARATOR +
				(this.error!=null ? this.error.getMessage() : "");
		return outline;
	}
	
	private int findTimestamp() {
		Pattern p = Pattern.compile(VERSION_TABLE_TIMESTAMP_REGEX);
		Matcher m = p.matcher(this.table.name());
		return m.find()?m.start():this.table.name().length();
	}
	
	public String metaInfos() {
		if (this.metaInfos == null) {
			boolean hasParams = this.table.hasParameters();
			String recordCount       = (hasParams ? this.table.parameters().getOrDefault("recordCount", ""):"");
			String averageRecordSize = (hasParams ? this.table.parameters().getOrDefault("averageRecordSize", ""):"");
			
			int nbRows = 0;
			if (!"".equals(recordCount)) {
				try {
					nbRows = Integer.parseInt(recordCount);
				} catch (NumberFormatException e) {
					nbRows = 0;
				}
			}
			
			int avgSize = 0;
			if (!"".equals(averageRecordSize)) {
				try {
					avgSize = Integer.parseInt(averageRecordSize);
				} catch (NumberFormatException e) {
					avgSize = 0;
				}
			}
			
			int nbCols = this.table.columns().size();
			int nbCells = nbCols * nbRows;
			int tableSize = nbRows * avgSize;
			
			this.metaInfos = nbRows
			   + SEPARATOR + avgSize
			   + SEPARATOR + nbCells
			   + SEPARATOR + tableSize
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("inputformat", ""):"")
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("compressionType", ""):"")
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("classification", ""):"")
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("serde.serialization.lib", ""):"")
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("outputformat", ""):"")
			   + SEPARATOR + (hasParams ? this.table.parameters().getOrDefault("location", ""):"");
		}
		return this.metaInfos;
	}
	
	public String headersInfos() {
		if (this.headersInfos == null) {
			int i = this.table.columns().size();
			Optional<Integer> j = this.table.columns().stream().map((Column col) -> {
				return col.name().matches("col\\d{1,4}")?1:0;
			}).reduce((result, match) -> {
				return result+match;
			});
			this.headersInfos = (j.equals(Optional.of(i))?"BADLY_INFERRED":"GRACELY_SET") + SEPARATOR + i;
		}
		return this.headersInfos;
	}

}

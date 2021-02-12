package fr.hshc.aws.audit;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.AthenaClientBuilder;

public class AthenaClientFactory {
		private final AthenaClientBuilder builder;
		public AthenaClientFactory(AwsCredentialsProvider pCredProvider, Region pRegion) {
			super();
			this.builder = AthenaClient.builder().region(pRegion).credentialsProvider(pCredProvider);
		}
		public AthenaClient createClient() {
			return this.builder.build();
		}
	}
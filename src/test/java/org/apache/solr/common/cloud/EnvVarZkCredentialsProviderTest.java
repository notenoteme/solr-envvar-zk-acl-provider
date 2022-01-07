package org.apache.solr.common.cloud;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvVarZkCredentialsProviderTest {

    private static final String CREDENTIALS_1 = "digest user1:12345";
    private static final String CREDENTIALS_2 = "x509 solr-staging";

    @Test
    void shouldLoadSingleSecurityCredential() {
        final EnvVarZkCredentialsProvider provider = new EnvVarZkCredentialsProvider(CREDENTIALS_1, "");
        assertThat(provider.getCredentials()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(createCredentials("digest", "user1:12345"));
    }

    @Test
    void shouldLoadSingleNonSecurityCredential() {
        final EnvVarZkCredentialsProvider provider = new EnvVarZkCredentialsProvider("", CREDENTIALS_1);
        assertThat(provider.getCredentials()).usingRecursiveFieldByFieldElementComparator()
                .containsExactly(createCredentials("digest", "user1:12345"));
    }

    private static ZkCredentialsProvider.ZkCredentials createCredentials(String scheme, String auth) {
        return new ZkCredentialsProvider.ZkCredentials(scheme, auth.getBytes(StandardCharsets.UTF_8));
    }
}

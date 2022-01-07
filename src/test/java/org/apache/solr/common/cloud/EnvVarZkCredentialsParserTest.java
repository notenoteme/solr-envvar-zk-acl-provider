package org.apache.solr.common.cloud;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvVarZkCredentialsParserTest {

    private static final String ACL_SINGLE = "digest user1:12345";
    private static final String ACL_MALFORMED = "digestuser1:12345";
    private static final String ACL_MULTIPLE = "digest user1:12345,x509 Zookeeper CLI,x509 solr-staging";
    private static final String ACL_SPACES = "x509 ZK CLI,x509 Peter Molnar";
    private static final String ACL_ONE_SPACE = " ";
    private static final String ACL_MISSING_AUTH = "x509 ";

    @Test
    void shouldLoadNullCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(null)).isEmpty();
    }

    @Test
    void shouldLoadEmptyCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar("")).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED)).isEmpty();
    }

    @Test
    void shouldIgnoreOneSpaceCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_ONE_SPACE)).isEmpty();
    }

    @Test
    void shouldIgnoreMissingAuthCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MISSING_AUTH)).isEmpty();
    }

    @Test
    void shouldLoadSingle() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_SINGLE))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        createCredentials("digest", "user1:12345"));
    }

    @Test
    void shouldLoadMultiple() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MULTIPLE))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        createCredentials("digest", "user1:12345"),
                        createCredentials("x509", "Zookeeper CLI"),
                        createCredentials("x509", "solr-staging"));
    }
    @Test
    void shouldLoadSpaces() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_SPACES))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        createCredentials("x509", "ZK CLI"),
                        createCredentials("x509", "Peter Molnar"));
    }


    private static ZkCredentialsProvider.ZkCredentials createCredentials(String scheme, String auth) {
        return new ZkCredentialsProvider.ZkCredentials(scheme, auth.getBytes(StandardCharsets.UTF_8));
    }
}

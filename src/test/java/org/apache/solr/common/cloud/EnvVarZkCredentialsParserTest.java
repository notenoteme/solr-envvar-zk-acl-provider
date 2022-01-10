package org.apache.solr.common.cloud;

import org.apache.zookeeper.ZooDefs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvVarZkCredentialsParserTest {

    private static final String ACL_EMPTY = ",,";
    private static final String ACL_SINGLE = "digest|user1:12345|rwcda";
    private static final String ACL_MALFORMED = "digestuser1:12345|";
    private static final String ACL_MALFORMED_NO_SCHEME = "|tuser1:12345|r";
    private static final String ACL_MALFORMED_NO_AUTH = "digest||r";
    private static final String ACL_MALFORMED_NO_PERMS = "digest|user1:12345|";
    private static final String ACL_MALFORMED_PERMS_TOO_LONG = "digest|user1:12345|crwdac";
    private static final String ACL_MALFORMED_INVALID_PERM = "digest|user1:12345|z";
    private static final String ACL_MULTIPLE = "digest|user1:12345|rwcda,x509|Zookeeper CLI|r,x509|solr-staging|crdwa";
    private static final String ACL_SPACES = "x509|ZK CLI|w,x509|Peter Molnar|r";
    private static final String ACL_JUST_SEPARATORS = "||";
    private static final String ACL_MISSING_AUTH = "x509|";

    @Test
    void shouldLoadNullCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(null)).isEmpty();
    }

    @Test
    void shouldLoadEmptyCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar("")).isEmpty();
    }

    @Test
    void shouldLoadEmpty2Credentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_EMPTY)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentialsNoScheme() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED_NO_SCHEME)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentialsNoAuth() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED_NO_AUTH)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentialsNoPerms() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED_NO_PERMS)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentialsPermsTooLong() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED_PERMS_TOO_LONG)).isEmpty();
    }

    @Test
    void shouldIgnoreMalformedCredentialsInvalidPerms() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MALFORMED_INVALID_PERM)).isEmpty();
    }

    @Test
    void shouldIgnoreOneSpaceCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_JUST_SEPARATORS)).isEmpty();
    }

    @Test
    void shouldIgnoreMissingAuthCredentials() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MISSING_AUTH)).isEmpty();
    }

    @Test
    void shouldLoadSingle() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_SINGLE))
                .containsExactly(
                        createCredentials("digest", "user1:12345", ZooDefs.Perms.ALL));
    }

    @Test
    void shouldLoadMultiple() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_MULTIPLE))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        createCredentials("digest", "user1:12345", ZooDefs.Perms.ALL),
                        createCredentials("x509", "Zookeeper CLI", ZooDefs.Perms.READ),
                        createCredentials("x509", "solr-staging", ZooDefs.Perms.ALL));
    }
    @Test
    void shouldLoadSpaces() {
        assertThat(EnvVarZkCredentialsParser.parseEnvVar(ACL_SPACES))
                .usingRecursiveFieldByFieldElementComparator()
                .containsExactly(
                        createCredentials("x509", "ZK CLI", ZooDefs.Perms.WRITE),
                        createCredentials("x509", "Peter Molnar", ZooDefs.Perms.READ));
    }

    @Test
    void shouldParseReadPerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("r")).isEqualTo(ZooDefs.Perms.READ);
    }

    @Test
    void shouldParseWritePerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("w")).isEqualTo(ZooDefs.Perms.WRITE);
    }

    @Test
    void shouldParseDeletePerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("d")).isEqualTo(ZooDefs.Perms.DELETE);
    }

    @Test
    void shouldParseCreatePerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("c")).isEqualTo(ZooDefs.Perms.CREATE);
    }

    @Test
    void shouldParseAdminPerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("a")).isEqualTo(ZooDefs.Perms.ADMIN);
    }

    @Test
    void shouldParseReadWritePerm() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("rw")).isEqualTo(ZooDefs.Perms.READ | ZooDefs.Perms.WRITE);
    }

    @Test
    void shouldParseAllPerms() {
        assertThat(EnvVarZkCredentialsParser.parsePermissions("crdwa")).isEqualTo(ZooDefs.Perms.ALL);
    }

    private static ZkEnvVarACL createCredentials(String scheme, String auth, int permissions) {
        return new ZkEnvVarACL(scheme, auth, permissions);
    }
}

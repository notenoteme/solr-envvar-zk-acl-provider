package org.apache.solr.common.cloud;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.solr.common.cloud.EnvVarZkACLProvider.SOLR_ZK_NON_SECURITY_ACLS;
import static org.apache.solr.common.cloud.EnvVarZkACLProvider.SOLR_ZK_SECURITY_ACLS;

public class EnvVarZkCredentialsProvider implements ZkCredentialsProvider {

    private final List<ZkCredentials> credentials;

    public EnvVarZkCredentialsProvider() {
        this(System.getenv(SOLR_ZK_SECURITY_ACLS), System.getenv(SOLR_ZK_NON_SECURITY_ACLS));
    }

    public EnvVarZkCredentialsProvider(final String solrZkSecurityAcls, final String solrZkNonSecurityAcls) {
        credentials = Stream.concat(
                        EnvVarZkCredentialsParser.parseEnvVar(solrZkSecurityAcls),
                        EnvVarZkCredentialsParser.parseEnvVar(solrZkNonSecurityAcls))
                .distinct()
                .map(zkAcl -> new ZkCredentials(zkAcl.getScheme(), zkAcl.getAuth().getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.toList());
    }

    @Override
    public Collection<ZkCredentials> getCredentials() {
        return credentials;
    }
}

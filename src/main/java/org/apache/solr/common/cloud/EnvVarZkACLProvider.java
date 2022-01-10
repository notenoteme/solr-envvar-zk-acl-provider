package org.apache.solr.common.cloud;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class EnvVarZkACLProvider extends SecurityAwareZkACLProvider {

    public static final String SOLR_ZK_SECURITY_ACLS = "SOLR_ZK_SECURITY_ACLS";
    public static final String SOLR_ZK_NON_SECURITY_ACLS = "SOLR_ZK_NON_SECURITY_ACLS";

    private final List<ACL> securityAcls;
    private final List<ACL> nonSecurityAcls;

    public EnvVarZkACLProvider() {
        this(System.getenv(SOLR_ZK_SECURITY_ACLS), System.getenv(SOLR_ZK_NON_SECURITY_ACLS));
    }

    public EnvVarZkACLProvider(String solrZkSecurityAcls, String solrZkNonSecurityAcls) {
        securityAcls = createAcls(solrZkSecurityAcls);
        nonSecurityAcls = createAcls(solrZkNonSecurityAcls);
    }

    private static List<ACL> createAcls(final String envVarValue) {
        return EnvVarZkCredentialsParser.parseEnvVar(envVarValue)
                .map(envVarACL -> new ACL(envVarACL.getPermissions(), new Id(
                        envVarACL.getScheme(),
                        envVarACL.getAuth())))
                .collect(Collectors.toList());
    }

    @Override
    protected List<ACL> createNonSecurityACLsToAdd() {
        return nonSecurityAcls;
    }

    @Override
    protected List<ACL> createSecurityACLsToAdd() {
        return securityAcls;
    }
}

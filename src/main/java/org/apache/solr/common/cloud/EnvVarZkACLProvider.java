package org.apache.solr.common.cloud;

import lombok.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EnvVarZkACLProvider extends SecurityAwareZkACLProvider {

    public static final String SOLR_ZK_SECURITY_ACLS = "SOLR_ZK_SECURITY_ACLS";
    public static final String SOLR_ZK_NON_SECURITY_ACLS = "SOLR_ZK_NON_SECURITY_ACLS";

    private final List<ACL> securityAcls;
    private final List<ACL> nonSecurityAcls;

    public EnvVarZkACLProvider() {
        securityAcls = createAcls(ZooDefs.Perms.ALL, System.getenv(SOLR_ZK_SECURITY_ACLS));
        nonSecurityAcls = createAcls(ZooDefs.Perms.READ, System.getenv(SOLR_ZK_NON_SECURITY_ACLS));
    }

    private static List<ACL> createAcls(final int perms, final String envVarValue) {
        return parseEnvVar(envVarValue)
                .map(zkIdentity -> new ACL(perms, new Id(zkIdentity.getScheme(), zkIdentity.getAuth())))
                .collect(Collectors.toList());
    }

    private static Stream<ZkIdentity> parseEnvVar(final String envVarValue) {
        final String[] identities = Optional.ofNullable(envVarValue)
                .map(s -> StringUtils.split(s, ','))
                .orElse(new String[0]);

        return Arrays.stream(identities)
                .map(EnvVarZkACLProvider::parseZkCredentials)
                .flatMap(Optional::stream);
    }

    private static Optional<ZkIdentity> parseZkCredentials(String credentials) {
        int spacePos = credentials.indexOf(' ');
        if (spacePos < 0) {
            return Optional.empty();
        }

        final String scheme = credentials.substring(0, spacePos);
        final String auth = credentials.substring(spacePos);

        if (StringUtils.isEmpty(scheme) || StringUtils.isEmpty(auth)) {
            return Optional.empty();
        }

        return Optional.of(new ZkIdentity(scheme, auth));
    }


    @Override
    protected List<ACL> createNonSecurityACLsToAdd() {
        return nonSecurityAcls;
    }

    @Override
    protected List<ACL> createSecurityACLsToAdd() {
        return securityAcls;
    }

    @Value
    private static class ZkIdentity {
        String scheme;
        String auth;
    }
}

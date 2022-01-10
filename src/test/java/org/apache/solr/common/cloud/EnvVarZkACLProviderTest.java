package org.apache.solr.common.cloud;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EnvVarZkACLProviderTest {

    private static final String ACL_1 = "digest|user1:12345|crdwa";
    private static final String ACL_MULTIPLE = "digest|user1:12345|crdwa,x509|Zookeeper CLI|r,x509|solr-staging|r";

    @Test
    void shouldLoadSingleSecurityAcl() {
        final ACL acl = createDigestACL("user1", "12345", ZooDefs.Perms.ALL);

        final EnvVarZkACLProvider aclProvider = new EnvVarZkACLProvider(ACL_1, null);
        assertThat(aclProvider.createSecurityACLsToAdd()).containsExactly(acl);
        assertThat(aclProvider.createNonSecurityACLsToAdd()).isEmpty();
    }

    @Test
    void shouldLoadSingleNonSecurityAcl() {
        final ACL acl = createDigestACL("user1", "12345", ZooDefs.Perms.ALL);

        final EnvVarZkACLProvider aclProvider = new EnvVarZkACLProvider(null, ACL_1);
        assertThat(aclProvider.createSecurityACLsToAdd()).isEmpty();
        assertThat(aclProvider.createNonSecurityACLsToAdd()).containsExactly(acl);
    }

    @Test
    void shouldLoadMultipleAcls() {
        final ACL[] acls = new ACL[]{
                createDigestACL("user1", "12345", ZooDefs.Perms.ALL),
                createX509ACL("Zookeeper CLI", ZooDefs.Perms.READ),
                createX509ACL("solr-staging", ZooDefs.Perms.READ)
        };

        final EnvVarZkACLProvider aclProvider = new EnvVarZkACLProvider(null, ACL_MULTIPLE);
        assertThat(aclProvider.createSecurityACLsToAdd()).isEmpty();
        assertThat(aclProvider.createNonSecurityACLsToAdd()).containsExactly(acls);
    }

    private static ACL createDigestACL(final String user, final String password, final int perms) {
        final ACL acl = new ACL();

        final Id id = new Id();
        id.setId(user + ":" + password);
        id.setScheme("digest");

        acl.setId(id);
        acl.setPerms(perms);

        return acl;
    }

    private static ACL createX509ACL(final String commonName, final int perms) {
        final ACL acl = new ACL();

        final Id id = new Id();
        id.setId(commonName);
        id.setScheme("x509");

        acl.setId(id);
        acl.setPerms(perms);

        return acl;
    }
}

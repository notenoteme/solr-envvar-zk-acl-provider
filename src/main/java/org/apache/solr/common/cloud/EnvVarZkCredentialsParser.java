package org.apache.solr.common.cloud;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.ZooDefs;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@Slf4j
public class EnvVarZkCredentialsParser {

    private static final Set<Character> PERMISSON_CHARS = Set.of('r', 'w', 'c', 'd', 'a');

    private EnvVarZkCredentialsParser() {
    }

    public static Stream<ZkEnvVarACL> parseEnvVar(final String value) {
        final String[] identities = Optional.ofNullable(value)
                .map(s -> StringUtils.split(s, ','))
                .orElse(new String[0]);

        return Arrays.stream(identities)
                .map(EnvVarZkCredentialsParser::parseAcl)
                .flatMap(Optional::stream);
    }

    public static Optional<ZkEnvVarACL> parseAcl(String credentials) {
        if (StringUtils.isEmpty(credentials)) {
            log.warn("Ignoring empty Zookeeper credentials");
            return Optional.empty();
        }

        String[] parts = StringUtils.split(credentials, '|');
        if (parts.length != 3) {
            log.warn("Ignoring malformed Zookeeper credentials: {}", credentials);
            return Optional.empty();
        }

        final String scheme = parts[0];
        final String auth = parts[1];
        final String permissions = parts[2];

        if (StringUtils.isEmpty(scheme) || StringUtils.isEmpty(auth) || StringUtils.isEmpty(permissions)) {
            log.warn("Ignoring malformed Zookeeper credentials: {}", credentials);
            return Optional.empty();
        }

        if (permissions.length() > 5 || !permissions.chars().allMatch(c -> PERMISSON_CHARS.contains((char) c))) {
            log.warn("Ignoring malformed Zookeeper credentials: {}", credentials);
            return Optional.empty();
        }

        return Optional.of(new ZkEnvVarACL(scheme, auth, parsePermissions(permissions)));
    }

    public static int parsePermissions(String permissions) {
        int perms = 0;

        if (permissions.indexOf('r') >= 0) {
            perms |= ZooDefs.Perms.READ;
        }
        if (permissions.indexOf('w') >= 0) {
            perms |= ZooDefs.Perms.WRITE;
        }
        if (permissions.indexOf('c') >= 0) {
            perms |= ZooDefs.Perms.CREATE;
        }
        if (permissions.indexOf('d') >= 0) {
            perms |= ZooDefs.Perms.DELETE;
        }
        if (permissions.indexOf('a') >= 0) {
            perms |= ZooDefs.Perms.ADMIN;
        }

        return perms;
    }

}

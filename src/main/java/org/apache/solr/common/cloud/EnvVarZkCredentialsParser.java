package org.apache.solr.common.cloud;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public class EnvVarZkCredentialsParser {

    private EnvVarZkCredentialsParser() {
    }

    public static Stream<ZkCredentialsProvider.ZkCredentials> parseEnvVar(final String value) {
        final String[] identities = Optional.ofNullable(value)
                .map(s -> StringUtils.split(s, ','))
                .orElse(new String[0]);

        return Arrays.stream(identities)
                .map(EnvVarZkCredentialsParser::parseZkCredentials)
                .flatMap(Optional::stream);
    }

    public static Optional<ZkCredentialsProvider.ZkCredentials> parseZkCredentials(String credentials) {
        int spacePos = credentials.indexOf(' ');
        if (spacePos < 0) {
            log.warn("Ignoring malformed Zookeeper credentials: {}", credentials);
            return Optional.empty();
        }

        final String scheme = credentials.substring(0, spacePos);
        final String auth = credentials.substring(spacePos + 1);

        if (StringUtils.isEmpty(scheme) || StringUtils.isEmpty(auth)) {
            log.warn("Ignoring malformed Zookeeper credentials: {}", credentials);
            return Optional.empty();
        }

        return Optional.of(new ZkCredentialsProvider.ZkCredentials(scheme, auth.getBytes(StandardCharsets.UTF_8)));
    }

}

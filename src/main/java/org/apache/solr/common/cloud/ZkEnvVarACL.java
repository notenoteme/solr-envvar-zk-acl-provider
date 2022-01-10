package org.apache.solr.common.cloud;

import lombok.Value;

@Value
public class ZkEnvVarACL {
    String scheme;
    String auth;
    int permissions;
}

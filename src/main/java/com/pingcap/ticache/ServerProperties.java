package com.pingcap.ticache;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.server")
public class ServerProperties {

    private int tcpPort;

    private int bossCount;

    private int workerCount;

    private boolean keepAlive;

    private int backlog;
}

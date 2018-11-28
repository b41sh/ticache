package com.pingcap.ticache;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.InetSocketAddress;

/**
 * Main Server
 *
 */
@Getter
@Setter
@RequiredArgsConstructor
@Component
public class MainServer {

    private final ServerBootstrap serverBootstrap;

    private final InetSocketAddress tcpPort;

    private Channel serverChannel;

    public void start() throws Exception {
        serverChannel = serverBootstrap.bind(tcpPort)
                .sync()
                .channel()
                .closeFuture()
                .sync()
                .channel();
    }

    @PreDestroy
    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
            serverChannel.parent().close();
        }
    }
}

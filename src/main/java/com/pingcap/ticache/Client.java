package com.pingcap.ticache;

import shade.com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.tikv.RawKVClient;

/**
 * tikv client wrapper
 *
 */
@Component
public class Client {

    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private RawKVClient client = null;

    public Client() {

        String address = "117.50.61.196:2379";
        client = RawKVClient.create(address);

        logger.info("init RawKVClient address=" + address + " client=" + client);
    }

    public void put(String key, String value) {

        try {
            logger.info("client put key=" + key + " val=" + value);
            ByteString bKey = ByteString.copyFrom(key, StandardCharsets.UTF_8);
            ByteString bValue = ByteString.copyFrom(value, StandardCharsets.UTF_8);
            client.put(bKey, bValue);
        } catch (Exception e) {
            logger.error("client put error", e);
        }
    }

    public String get(String key) {

        try {
            logger.info("client get key=" + key);
            ByteString bKey = ByteString.copyFrom(key, StandardCharsets.UTF_8);
            ByteString bValue = client.get(bKey);
            return bValue.toStringUtf8();
        } catch (Exception e) {
            logger.error("client get error", e);
        }

        return null;
    }
}

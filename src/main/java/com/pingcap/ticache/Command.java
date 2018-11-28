package com.pingcap.ticache;

import lombok.Getter;
import lombok.Setter;

/**
 * Memcache command
 *
 */
@Getter
@Setter
public class Command {

    private String cmd;

    private String key;

    private int flags;

    private int ttl;

    private int size;

    private String val;

    public Command(String cmd, String key, int flags, int ttl, int size, String val) {
        this.cmd = cmd;
        this.key = key;
        this.flags = flags;
        this.ttl = ttl;
        this.size = size;
        this.val = val;
    }
}

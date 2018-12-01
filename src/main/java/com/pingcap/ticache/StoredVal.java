package com.pingcap.ticache;

import lombok.Getter;
import lombok.Setter;

/**
 * Memcache command
 *
 */
@Getter
@Setter
public class StoredVal {

    private int flags;

    private int ttl;

    private int size;

    private String val;

    public StoredVal(int flags, int ttl, int size, String val) {
        this.flags = flags;
        this.ttl = ttl;
        this.size = size;
        this.val = val;
    }

    public StoredVal(String fullVal) {

        String[] lines = fullVal.split("\\r?\\n");

        String extVal = lines[0];
        String[] subLines = extVal.split(" ");
        this.flags = Integer.parseInt(subLines[0]);
        this.ttl = Integer.parseInt(subLines[1]);
        this.size = Integer.parseInt(subLines[2]);

        StringBuilder valSb = new StringBuilder();
        int len = lines.length;
        for (int i = 1; i < len - 1; i++) {
            valSb.append(lines[i]);
            valSb.append("\r\n");
        }
        valSb.append(lines[len - 1]);
        this.val = valSb.toString();
    }

    public String getFullVal() {
        StringBuilder sb = new StringBuilder();
        sb.append(flags);
        sb.append(" ");
        sb.append(ttl);
        sb.append(" ");
        sb.append(size);
        sb.append("\r\n");
        sb.append(val);

        return sb.toString();
    }
}

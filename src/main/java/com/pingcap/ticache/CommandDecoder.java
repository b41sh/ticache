package com.pingcap.ticache;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.io.IOException;
import java.util.List;

/**
 * Decodes the Memcache protocol into Command
 * https://github.com/memcached/memcached/blob/master/doc/protocol.txt
 * https://lzone.de/cheat-sheet/memcached
 *
 * get key
 * set key flags ttl size\r\ndata
 * add newkey flags ttl size\r\ndata
 * replace key flags ttl size\r\ndata
 * append key flags ttl size\r\ndata
 * prepend key flags ttl size\r\ndata
 * incr key val
 * decr key val
 * delete key
 * flush_all
 * flush_all seconds
 * stats
 * version
 * verbosity
 * quit
 *
 */
public class CommandDecoder extends ReplayingDecoder<Void> {

    private String cmd;

    private String key;

    private int flags;

    private int ttl;

    private int size;

    private String val;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        ByteBuf buffer = in.readBytes(3);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buffer.capacity(); i++) {
            char c = (char)buffer.getByte(i);
            sb.append(c);
        }

        if ("get".equals(sb.toString())) {
            decodeGet(in);
        } else if ("set".equals(sb.toString())) {
            decodeSet(in);
        }
        // @todo 

        sendCmdToHandler(out);
    }

    private void decodeGet(ByteBuf in) {

        this.cmd = "get";

        in.skipBytes(1);

        this.key = readString(in);

        this.flags = readInt(in);

        this.ttl = readInt(in);

        this.size = readInt(in);

        System.out.println(this.cmd + " " + this.key + " " + this.flags + " " + this.ttl + " " + this.size);
    }

    private void decodeSet(ByteBuf in) {
        this.cmd = "set";

        in.skipBytes(1);
        this.key = readString(in);

    }

    private void sendCmdToHandler(List<Object> out) {
        System.out.println("send");
        out.add(new Command(cmd, key, flags, ttl, size, val));
    }


    private String readString(ByteBuf in) {

        char c;
        StringBuilder sb = new StringBuilder();
        while ((c = (char) in.readByte()) != ' ') {
            sb.append(c);
        }

        return sb.toString();
    }

    private int readInt(ByteBuf in) {

        char c;
        int integer = 0;
        while ((c = (char) in.readByte()) != ' ') {
            integer = (integer * 10) + (c - '0');
        }
        return integer;
    }
}

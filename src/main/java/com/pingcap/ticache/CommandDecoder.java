package com.pingcap.ticache;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
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

    private static Logger logger = LoggerFactory.getLogger(CommandDecoder.class);

    private String cmd;

    private String key;

    private int flags;

    private int ttl;

    private int size;

    private StringBuilder valSb;

    private String val;

    public static final String[] cmds = new String[] {
                                            "get", "set", "add",
                                            "incr", "decr", "quit",
                                            "stats\r\n",
                                            "append", "delete",
                                            "replace", "prepend", "version\r\n",
                                            "flush_all\r\n", "verbosity\r\n"};


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        System.out.println("cmd=" + cmd);
        if (cmd == null) {
            char c;
            StringBuilder sb = new StringBuilder();
            while (true) {
                c = (char) in.readByte();
                if (c == ' ') {
                    break;
                }
                sb.append(c);
                if (sb.lastIndexOf("\r") > 0) {
                    c = (char) in.readByte();
                    sb.append(c);
                    break;
                }
            }
            String tmpCmd = sb.toString();
            logger.info("tmpCmd=" + tmpCmd);
            if (Arrays.stream(cmds).anyMatch(sb.toString()::equals)) {
                cmd = tmpCmd;
                logger.error("found cmd=" + cmd);
            } else {
                logger.error("tmpCmd error " + tmpCmd);
                ByteBuf outBuf = Unpooled.copiedBuffer("ERROR\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            }

            if (cmd.equals("version\r\n")) {
                showVersion(ctx);
            } else if (cmd.equals("flush_all\r\n")) {
                Command command = new Command("flush_all", key, flags, ttl, size, val);
                clean();
                out.add(command);
            } else if (cmd.equals("get")) {
                decodeGet(in);
                Command command = new Command(cmd, key, flags, ttl, size, val);
                clean();
                out.add(command);
            } else if (cmd.equals("set") || cmd.equals("add") || cmd.equals("replace")
                            || cmd.equals("append") || cmd.equals("prepend")) {
                decodeSet(in);
            } else if (cmd.equals("incr") || cmd.equals("decr")) {
                decodeIncrDecr(in);
                Command command = new Command(cmd, key, flags, ttl, size, val);
                clean();
                out.add(command);
            } else if (cmd.equals("delete")) {
                decodeDelete(in);
                Command command = new Command(cmd, key, flags, ttl, size, val);
                clean();
                out.add(command);
            } else {
                clean();
                ByteBuf outBuf = Unpooled.copiedBuffer("ERROR\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            }
        } else {
            String curVal = readEndString(in);
            this.valSb.append(curVal);

            if (this.valSb.length() < this.size + 2) {
                checkpoint();
            } else if (this.valSb.length() == this.size + 2) {
                this.val = this.valSb.toString();
                Command command = new Command(cmd, key, flags, ttl, size, val);
                clean();
                out.add(command);
            } else {
                clean();
                ByteBuf outBuf = Unpooled.copiedBuffer("CLIENT_ERROR bad data chunk\r\nERROR\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            }
        }
    }

    private void clean() {
        this.cmd = null;
        this.key = null;
        this.cmd = null;
        this.flags = 0;
        this.ttl = 0;
        this.size = 0;
        this.valSb = null;
        this.val = null;
    }

    private void showVersion(ChannelHandlerContext ctx) {

        logger.info("show version");
        ByteBuf outBuf = Unpooled.copiedBuffer("TICACHE VERSION 1.0.0\r\n".getBytes());
        ctx.writeAndFlush(outBuf);
    }

    private void decodeSet(ByteBuf in) {

        this.key = readString(in);

        this.flags = readInt(in);

        this.ttl = readInt(in);

        this.size = readEndInt(in);

        this.valSb = new StringBuilder();

        logger.info("read cmd " + this.cmd + " " + this.key + " " + this.flags + " " + this.ttl + " " + this.size);

        checkpoint();
    }

    private void decodeGet(ByteBuf in) {
        String key = readEndString(in);
        key = key.substring(0, key.length() - 2);
        this.key = key;
    }

    private void decodeDelete(ByteBuf in) {
        String key = readEndString(in);
        key = key.substring(0, key.length() - 2);
        this.key = key;
    }

    private void decodeIncrDecr(ByteBuf in) {
        this.key = readString(in);

        this.val = String.valueOf(readEndInt(in));
    }

    private String readString(ByteBuf in) {

        char c;
        StringBuilder sb = new StringBuilder();
        while ((c = (char) in.readByte()) != ' ') {
            sb.append(c);
        }

        return sb.toString();
    }

    private String readEndString(ByteBuf in) {

        char c;
        StringBuilder sb = new StringBuilder();
        while (true) {
            c = (char) in.readByte();
            sb.append(c);
            if (sb.lastIndexOf("\r") > 0) {
                c = (char) in.readByte();
                sb.append(c);
                break;
            }
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

    private int readEndInt(ByteBuf in) {

        char c;
        int integer = 0;
        while ((c = (char) in.readByte()) != '\r') {
            if (c == ' ') {
                while ((c = (char) in.readByte()) != '\r') {
                    continue;
                }
                break;
            }
            integer = (integer * 10) + (c - '0');
        }
        in.skipBytes(1);
        return integer;
    }

}

package com.pingcap.ticache;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.RequiredArgsConstructor;
import io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * Handle decoded commands
 */
@Component
@RequiredArgsConstructor
@ChannelHandler.Sharable
public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    private static Logger logger = LoggerFactory.getLogger(CommandHandler.class);

    private final Client client;

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {

        String cmd = msg.getCmd();
        logger.info("command handler cmd=" + cmd);

        try {
            if (cmd.equals("set")) {
                doSet(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
                ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            } else if (cmd.equals("add")) {
                boolean ret = doAdd(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
                if (ret) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("replace")) {
                boolean ret = doReplace(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
                if (ret) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("prepend")) {
                boolean ret = doPrepend(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
                if (ret) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("append")) {
                boolean ret = doAppend(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
                if (ret) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("incr")) {
                try {
                    boolean ret = doIncr(msg.getKey(), msg.getVal());
                    if (ret) {
                        ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                        ctx.writeAndFlush(outBuf);
                    } else {
                        ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                        ctx.writeAndFlush(outBuf);
                    }
                } catch (Exception e) {
                    ByteBuf outBuf = Unpooled.copiedBuffer(e.getMessage().getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("decr")) {
                try {
                    boolean ret = doDecr(msg.getKey(), msg.getVal());
                    if (ret) {
                        ByteBuf outBuf = Unpooled.copiedBuffer("STORED\r\n".getBytes());
                        ctx.writeAndFlush(outBuf);
                    } else {
                        ByteBuf outBuf = Unpooled.copiedBuffer("NOT_STORED\r\n".getBytes());
                        ctx.writeAndFlush(outBuf);
                    }
                } catch (Exception e) {
                    ByteBuf outBuf = Unpooled.copiedBuffer(e.getMessage().getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("get")) {
                String retVal = doGet(msg.getKey());
                if (retVal == null) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("END\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer(retVal.getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("delete")) {
                boolean ret = doDelete(msg.getKey());
                if (ret) {
                    ByteBuf outBuf = Unpooled.copiedBuffer("DELETED\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                } else {
                    ByteBuf outBuf = Unpooled.copiedBuffer("NOT_FOUND\r\n".getBytes());
                    ctx.writeAndFlush(outBuf);
                }
            } else if (cmd.equals("flush_all")) {
                doFlushAll();
                ByteBuf outBuf = Unpooled.copiedBuffer("OK\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            } else {
                ByteBuf outBuf = Unpooled.copiedBuffer("ERROR\r\n".getBytes());
                ctx.writeAndFlush(outBuf);
            }
        } catch (Exception e) {
            logger.error("cmd error", e);

            ByteBuf outBuf = Unpooled.copiedBuffer("SERVER_ERROR\r\n".getBytes());
            ctx.writeAndFlush(outBuf);
        }

    }

    private String doGet(String key) throws Exception {
        String oldVal = client.getData(key);
        if (oldVal == null || oldVal.length() == 0) {
            return null;
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int flags = storedVal.getFlags();
        int ttl = storedVal.getTtl();
        int size = storedVal.getSize();
        String val = storedVal.getVal();

        int currTime = (int) (System.currentTimeMillis() / 1000);
        if (ttl < currTime && ttl > 0) {
            client.deleteData(key);
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("VALUE ");
        sb.append(key);
        sb.append(" ");
        sb.append(flags);
        sb.append(" ");
        sb.append(size);
        sb.append("\r\n");
        sb.append(val);
        sb.append("END\r\n");

        return sb.toString();
    }

    private boolean doAdd(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.getData(key);

        boolean isValid = isValid(oldVal);
        if (isValid) {
            return false;
        }

        doSet(key, flags, ttl, size, val);
        return true;
    }

    private boolean doReplace(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.getData(key);
        boolean isValid = isValid(oldVal);
        if (!isValid) {
            return false;
        }

        doSet(key, flags, ttl, size, val);
        return true;
    }

    private boolean doAppend(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.getData(key);
        boolean isValid = isValid(oldVal);
        if (!isValid) {
            return false;
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();
        int newSize = storedVal.getSize() + size;
        StringBuilder newValSb = new StringBuilder();
        String oVal = storedVal.getVal();
        newValSb.append(oVal.substring(0, oVal.length() - 2));
        newValSb.append(val);
        String newVal = newValSb.toString();

        doSet(key, newFlags, newTtl, newSize, newVal);
        return true;
    }

    private boolean doPrepend(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.getData(key);
        boolean isValid = isValid(oldVal);
        if (!isValid) {
            return false;
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();
        int newSize = storedVal.getSize() + size;
        StringBuilder newValSb = new StringBuilder();
        newValSb.append(val.substring(0, val.length() - 2));
        newValSb.append(storedVal.getVal());
        String newVal = newValSb.toString();

        doSet(key, newFlags, newTtl, newSize, newVal);
        return true;
    }

    private boolean doIncr(String key, String val) throws Exception {
        String oldVal = client.getData(key);
        boolean isValid = isValid(oldVal);
        if (!isValid) {
            return false;
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();

        int tmpVal = 0;
        try {
            int nVal = Integer.parseInt(val);
            String ooVal = storedVal.getVal();
            int oVal = Integer.parseInt(ooVal.substring(0, ooVal.length() - 2));
            tmpVal = oVal + nVal;
        } catch (NumberFormatException e) {
            throw new Exception("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
        }

        String newVal = Integer.toString(tmpVal);
        int newSize = newVal.length();
        newVal += "\r\n";

        doSet(key, newFlags, newTtl, newSize, newVal);
        return true;
    }

    private boolean doDecr(String key, String val) throws Exception {
        String oldVal = client.getData(key);
        boolean isValid = isValid(oldVal);
        if (!isValid) {
            return false;
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();

        int tmpVal = 0;
        try {
            int nVal = Integer.parseInt(val);
            String ooVal = storedVal.getVal();
            int oVal = Integer.parseInt(ooVal.substring(0, ooVal.length() - 2));
            tmpVal = oVal - nVal;
        } catch (NumberFormatException e) {
            throw new Exception("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n");
        }
        String newVal = Integer.toString(tmpVal);
        int newSize = newVal.length();
        if (tmpVal < 0) {
            newVal = "0";
            newSize = storedVal.getSize();
        }
        newVal += "\r\n";

        doSet(key, newFlags, newTtl, newSize, newVal);
        return true;
    }

    private void doSet(String key, int flags, int ttl, int size, String val) throws Exception {
        logger.info("command handler doSet key=" + key + " flags=" + flags + " ttl=" + ttl + " size=" + size + " val=" + val);

        if (ttl <= 2592000 && ttl > 0) {
            int currTime = (int) (System.currentTimeMillis() / 1000);
            ttl += currTime;
        }

        StoredVal storedVal = new StoredVal(flags, ttl, size, val);
        logger.info("command handler doSet key=" + key + " flags=" + flags + " ttl=" + ttl + " size=" + size + " val=" + val);

        String fullVal = storedVal.getFullVal();
        logger.info("command handler put key=" + key + " fullVal=" + fullVal);
        client.putData(key, fullVal);

        doAddKey(key);
    }

    private boolean doDelete(String key) throws Exception {
        String oldVal = client.getData(key);
        logger.info("command handler doDelete key=" + key);

        boolean isValid = isValid(oldVal);
        if (!isValid) {
            client.deleteData(key);
            return false;
        }

        client.deleteData(key);
        return true;
    }

    private boolean isValid(String val) {
        if (val == null || val.length() == 0) {
            return false;
        }
        StoredVal storedVal = new StoredVal(val);
        int ttl = storedVal.getTtl();

        int currTime = (int) (System.currentTimeMillis() / 1000);
        if (ttl < currTime && ttl > 0) {
            return false;
        }
        return true;
    }

    private void doFlushAll() throws Exception {
        String curKeyCnt = "currKey";
        String curKeyNum = client.get(curKeyCnt);
        if (curKeyNum == null || curKeyNum.length() == 0) {
            return;
        }
        for (int i = 0; i <= Integer.parseInt(curKeyNum); i++) {
            String keySuffix = "k" + i;
            String curKey = client.get(keySuffix);
            client.delete(keySuffix);
            client.deleteData(curKey);
        }
        client.put(curKeyCnt, "0");
    }

    private void doAddKey(String addkey) throws Exception {
        String curKeyCnt = "currKey";
        String curKeyNum = client.get(curKeyCnt);
        if (curKeyNum == null || curKeyNum.length() == 0) {
            curKeyNum = "0";
        }
        String keySuffix = "k" + curKeyNum;
        client.put(keySuffix, addkey);

        int num = Integer.parseInt(curKeyNum);
        num++;
        client.put(curKeyCnt, String.valueOf(num));
    }
}

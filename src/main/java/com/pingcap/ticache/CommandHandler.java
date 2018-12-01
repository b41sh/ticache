package com.pingcap.ticache;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
//import org.apache.log4j.Logger;

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
//@Slf4j
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
            if (cmd.equals("get")) {
                doSet(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
            } else if (cmd.equals("add")) {
                doAdd(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
            } else if (cmd.equals("replace")) {
                doReplace(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
            } else if (cmd.equals("prepend")) {
                doPrepend(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
            } else if (cmd.equals("append")) {
                doAppend(msg.getKey(), msg.getFlags(), msg.getTtl(), msg.getSize(), msg.getVal());
            } else if (cmd.equals("incr")) {
                doIncr(msg.getKey(), msg.getVal());
            } else if (cmd.equals("decr")) {
                doDecr(msg.getKey(), msg.getVal());
            } else if (cmd.equals("get")) {
                doGet(msg.getKey());
            } else if (cmd.equals("delete")) {
                doDelete(msg.getKey());
            }
        } catch (Exception e) {
            logger.error("cmd error", e);
        }

    }

    private String doGet(String key) throws Exception {
        String oldVal = client.get(key);
        if (oldVal != null) {
            throw new Exception("EXISTS");
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int flags = storedVal.getFlags();
        int ttl = storedVal.getTtl();
        int size = storedVal.getSize();
        String val = storedVal.getVal();

        int currTime = (int) (System.currentTimeMillis() / 1000);
        if (ttl < currTime) {
            client.delete(key);
        }

        return val;
    }

    private void doAdd(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal != null) {
            throw new Exception("EXISTS");
        }

        doSet(key, flags, ttl, size, val);
    }

    private void doReplace(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal == null) {
            throw new Exception("NOT_FOUND");
        }

        doSet(key, flags, ttl, size, val);
    }

    private void doAppend(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal == null) {
            throw new Exception("NOT_STORED");
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();
        int newSize = storedVal.getSize() + size;
        StringBuilder newValSb = new StringBuilder();
        newValSb.append(storedVal.getVal());
        newValSb.append(val);
        String newVal = newValSb.toString();

        doSet(key, newFlags, newTtl, newSize, newVal);
    }

    private void doPrepend(String key, int flags, int ttl, int size, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal == null) {
            throw new Exception("NOT_STORED");
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();
        int newSize = storedVal.getSize() + size;
        StringBuilder newValSb = new StringBuilder();
        newValSb.append(val);
        newValSb.append(storedVal.getVal());
        String newVal = newValSb.toString();

        doSet(key, newFlags, newTtl, newSize, newVal);
    }

    private void doIncr(String key, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal == null) {
            throw new Exception("NOT_STORED");
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();

        int tmpVal = 0;
        try {
            int nVal = Integer.parseInt(val);
            int oVal = Integer.parseInt(storedVal.getVal());
            tmpVal = oVal + nVal;
        } catch (NumberFormatException e) {
            throw new Exception("CLIENT_ERROR cannot increment or decrement non-numeric value");
        }

        String newVal = Integer.toString(tmpVal);
        int newSize = newVal.length();

        doSet(key, newFlags, newTtl, newSize, newVal);

    }

    private void doDecr(String key, String val) throws Exception {
        String oldVal = client.get(key);
        if (oldVal == null) {
            throw new Exception("NOT_STORED");
        }
        StoredVal storedVal = new StoredVal(oldVal);
        int newFlags = storedVal.getFlags();
        int newTtl = storedVal.getTtl();

        int tmpVal = 0;
        try {
            int nVal = Integer.parseInt(val);
            int oVal = Integer.parseInt(storedVal.getVal());
            tmpVal = oVal - nVal;
        } catch (NumberFormatException e) {
            throw new Exception("CLIENT_ERROR cannot increment or decrement non-numeric value");
        }
        String newVal = Integer.toString(tmpVal);
        int newSize = newVal.length();
        if (tmpVal < 0) {
            newVal = "0";
            newSize = storedVal.getSize();
        }

        doSet(key, newFlags, newTtl, newSize, newVal);
    }

    private void doSet(String key, int flags, int ttl, int size, String val) throws Exception {
        if (ttl <= 2592000) {
            long time = System.currentTimeMillis();
            int mtime = (int) (time / 1000);
            ttl += mtime;
        }

        StoredVal storedVal = new StoredVal(flags, ttl, size, val);

        String fullVal = storedVal.getFullVal();
        logger.info("command handler put key=" + key + " fullVal=" + fullVal);
        client.put(key, fullVal);
    }

    private void doDelete(String key) throws Exception {
        String oldVal = client.get(key);
        if (oldVal != null) {
            throw new Exception("EXISTS");
        }
        client.delete(key);
    }

}

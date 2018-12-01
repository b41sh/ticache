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

        if (cmd.equals("get")) {
            String key = msg.getKey();
            logger.info("command handler get key=" + key);
            String retVal = client.get(key);
            logger.info("command handler get retVal=" + retVal);
        } else if (cmd.equals("set")) {
            String key = msg.getKey();
            String val = msg.getVal();
            logger.info("command handler put key=" + key + " val=" + val);
            client.put(key, val);
        }
    }
}

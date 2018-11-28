package com.pingcap.ticache;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Handle decoded commands
 */
@ChannelHandler.Sharable
public class CommandHandler extends SimpleChannelInboundHandler<Command> {

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command msg) throws Exception {

        String cmd = msg.getCmd();
        System.out.println("cmd=" + cmd);

        // @todo connect to tikv
    }
}

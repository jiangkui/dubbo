/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.netty4;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.IOException;
import java.util.List;

/**
 * NettyCodecAdapter.
 */
final public class NettyCodecAdapter {

    // 编码器
    private final ChannelHandler encoder = new InternalEncoder();

    // 解码器
    private final ChannelHandler decoder = new InternalDecoder();

    // 编解码协议：有 Dubbo 协议、Thrift 协议
    private final Codec2 codec;

    private final URL url;

    private final org.apache.dubbo.remoting.ChannelHandler handler;

    public NettyCodecAdapter(Codec2 codec, URL url, org.apache.dubbo.remoting.ChannelHandler handler) {
        this.codec = codec;
        this.url = url;
        this.handler = handler;
    }

    public ChannelHandler getEncoder() {
        return encoder;
    }

    public ChannelHandler getDecoder() {
        return decoder;
    }

    /**
     * 内部的一个 Netty 编码器
     */
    private class InternalEncoder extends MessageToByteEncoder {

        /**
         * 编码：对象 --> byte
         * @param ctx 上下文
         * @param msg 对象
         * @param out 输出流
         */
        @Override
        protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
            // 例如：Long 类型的编码器
            // out.writeLong(Long.parseLong(msg.toString()));

            org.apache.dubbo.remoting.buffer.ChannelBuffer buffer = new NettyBackedChannelBuffer(out);
            Channel ch = ctx.channel();
            NettyChannel channel = NettyChannel.getOrAddChannel(ch, url, handler);
            // 使用具体的协议进行编码
            // 后续流程如何？ 如何找到对应的 Service 发送？
            codec.encode(channel, buffer, msg);
        }
    }

    /**
     * 内部的一个 Netty 解码器
     */
    private class InternalDecoder extends ByteToMessageDecoder {

        /**
         * 解码：byte --> 对象
         * @param ctx 上下文
         * @param input 入站的 ByteBuf
         * @param out list集合，将解码后的数据传给下一个 Handler
         */
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> out) throws Exception {
            // 例如：Long 类型的解码器，long 是8个字节，buffer满8个字节后，在读取。
            // if (input.readableBytes() >= 8) {
            //     out.add(input.readLong());
            // }

            ChannelBuffer message = new NettyBackedChannelBuffer(input);

            NettyChannel channel = NettyChannel.getOrAddChannel(ctx.channel(), url, handler);

            // decode object.
            do {
                int saveReaderIndex = message.readerIndex();
                // 进行解码，后续流程如何？
                // response 如何找到对应的 Invoker 执行？
                Object msg = codec.decode(channel, message);
                if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                    message.readerIndex(saveReaderIndex);
                    break;
                } else {
                    //is it possible to go here ?
                    if (saveReaderIndex == message.readerIndex()) {
                        throw new IOException("Decode without read data.");
                    }
                    if (msg != null) {
                        out.add(msg);
                    }
                }
            } while (message.readable());
        }
    }
}

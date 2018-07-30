/**
Copyright 2018 BarD Software s.r.o

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.bardsoftware.papeeria.backend.cosmas

import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.bardsoftware.papeeria.backend.cosmas.CosmasGrpc.*
import com.google.protobuf.ByteString
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

private val LOG = LoggerFactory.getLogger("CosmasClient")

/**
 * Simple client that will send a request to Cosmas server and wait for response
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasClient(host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext(true)
            .build()

    private val blockingStub = newBlockingStub(this.channel)

    fun getVersion(version: Long) {
        LOG.info("Ask for version: $version")
        addText()
        val request: GetVersionRequest = GetVersionRequest.newBuilder()
                .setGeneration(version)
                .setFileId("43")
                .build()
        val response: GetVersionResponse = this.blockingStub.getVersion(request)
        LOG.info("Get file: ${response.file.content.toStringUtf8()}")
    }

    private fun addText() {
        val request = CreateVersionRequest.newBuilder()
                .setFileId("43")
                .setFile(ByteString.copyFromUtf8("ver0"))
                .build()
        this.blockingStub.createVersion(request)
    }

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

fun main(args: Array<String>) {
    val LOG = LoggerFactory.getLogger("client main")
    val arg = CosmasClientArgs(ArgParser(args))
    LOG.info("Try to bind in host ${arg.serverHost} and port ${arg.serverPort}")
    val client = CosmasClient(arg.serverHost, arg.serverPort)
    LOG.info("Start working in host ${arg.serverHost} and port ${arg.serverPort}")
    try {
        client.getVersion(0)
    } finally {
        client.shutdown()
    }
}

class CosmasClientArgs(parser: ArgParser) {
    val serverPort: Int by parser.storing("--server-port", help = "choose port of server where client will send requests")
    { toInt() }.default { 50051 }
    val serverHost: String by parser.storing("--server-host", help = "choose host of server where client will send requests")
            .default { "localhost" }
}
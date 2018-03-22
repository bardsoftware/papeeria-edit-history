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
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit

/**
 * Simple client that will send a request to Cosmas server and wait for response
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasClient(host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext(true)
            .build()
    private val blockingStub = newBlockingStub(channel)

    fun getVersion(version: Int) {
        println("Ask for version: $version")
        val request: GetVersionRequest = GetVersionRequest.newBuilder().setVersion(version).build()
        val response: GetVersionResponse = blockingStub.getVersion(request)
        println("Get text: ${response.file}")
    }

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

fun main(args: Array<String>) {
    val arg = CosmasClientArgs(ArgParser(args))
    println("Try to bind in host ${arg.serverHost} and port ${arg.serverPort}")
    val client = CosmasClient(arg.serverHost, arg.serverPort)
    println("Start working in host ${arg.serverHost} and port ${arg.serverPort}")
    try {
        client.getVersion(0)
    } finally {
        client.shutdown()
    }
}

class CosmasClientArgs(parser: ArgParser) {
    val serverPort: Int by parser.storing("--server-port", help = "choose server port")
    { toInt() }.default { 50051 }
    val serverHost: String by parser.storing("--server-host", help = "choose server host")
            .default { "localhost" }
}
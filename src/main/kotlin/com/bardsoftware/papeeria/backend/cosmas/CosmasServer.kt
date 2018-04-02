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

import io.grpc.Server
import io.grpc.ServerBuilder
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default

/**
 * Simple server that will wait for request and will send response back.
 * It uses CosmasGoogleCloudService or CosmasInMemoryService to store files
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasServer(port: Int, val service: CosmasGrpc.CosmasImplBase) {
    private val server: Server = ServerBuilder
            .forPort(port)
            .addService(service)
            .build()

    fun start() {
        server.start()
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                this@CosmasServer.stop()
            }
        })
    }

    private fun stop() {
        server.shutdown()
    }

    fun blockUntilShutDown() {
        server.awaitTermination()
    }
}

fun main(args: Array<String>) {
    val arg = CosmasServerArgs(ArgParser(args))
    println("Try to bind in port ${arg.port}")
    val server =
            if (arg.bucket != "")
                CosmasServer(arg.port, CosmasGoogleCloudService(arg.bucket))
            else
                CosmasServer(arg.port, CosmasInMemoryService())
    println("Start working in port ${arg.port}")
    server.start()
    server.blockUntilShutDown()
}

class CosmasServerArgs(parser: ArgParser) {
    val port: Int by parser.storing("--port", help = "choose port") { toInt() }.default { 50051 }
    val bucket: String by parser.storing("--bucket", help = "choose bucket")
}

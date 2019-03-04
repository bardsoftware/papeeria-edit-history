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

import com.google.common.base.Preconditions
import io.grpc.Server
import io.grpc.ServerBuilder
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import org.slf4j.LoggerFactory
import java.io.File

private val LOG = LoggerFactory.getLogger("CosmasServer")

/**
 * Simple server that will wait for request and will send response back.
 * It uses CosmasGoogleCloudService or CosmasInMemoryService to store files
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasServer(port: Int, val service: CosmasGrpc.CosmasImplBase) {

    constructor(port: Int, service: CosmasGrpc.CosmasImplBase,
                certChain: File, privateKey: File) : this(port, service) {
        Preconditions.checkState(certChain.exists(), "SSL certificate file doesn't exists: %s", certChain)
        Preconditions.checkState(privateKey.exists(), "SSL key file doesn't exists: %s", privateKey)
        this.server = ServerBuilder
                .forPort(port)
                .useTransportSecurity(certChain, privateKey)
                .addService(service)
                .build()
    }


    private var server: Server = ServerBuilder
            .forPort(port)
            .addService(service)
            .build()


    fun start() {
        this.server.start()
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                this@CosmasServer.stop()
            }
        })
    }

    private fun stop() {
        this.server.shutdown()
    }

    fun blockUntilShutDown() {
        this.server.awaitTermination()
    }
}


fun main(args: Array<String>) = mainBody {
    val parser = ArgParser(args)
    val arg = CosmasServerArgs(parser)
    val bucket = arg.bucket

    if (bucket == null) {
        LOG.error("Please cpecify --bucket argument to run GCS Cosmas implementation")
        return@mainBody
    }
    val server =
            if (arg.certChain != null && arg.privateKey != null) {
                LOG.info("Starting Cosmas in SECURE mode")
                CosmasServer(arg.port,
                        CosmasGoogleCloudService(bucket),
                        File(arg.certChain),
                        File(arg.privateKey))
            } else {
                LOG.info("Starting Cosmas in INSECURE mode")
                CosmasServer(arg.port,
                        CosmasGoogleCloudService(bucket))
            }

    LOG.info("Listening on port ${arg.port}")
    server.start()
    server.blockUntilShutDown()
}

class CosmasServerArgs(parser: ArgParser) {
    val port: Int by parser.storing("--port",
            help = "choose port that server will listen to, 50051 by default") { toInt() }.default { 50051 }
    val certChain: String? by parser.storing("--cert",
            help = "choose path to SSL cert").default { null }
    val privateKey: String? by parser.storing("--key",
            help = "choose path to SSL key").default { null }
    val bucket: String? by parser.storing("--free-bucket",
            help = "choose bucket").default { null }
}

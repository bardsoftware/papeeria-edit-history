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
import com.google.common.collect.Queues
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import com.xenomachina.argparser.mainBody
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.internal.GrpcUtil
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

private val LOG = LoggerFactory.getLogger("CosmasServer")

/**
 * Simple server that will wait for request and will send response back.
 * It uses CosmasGoogleCloudService or CosmasInMemoryService to store files
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasServer(port: Int, val service: CosmasGrpc.CosmasImplBase, certChain: File? = null, privateKey: File? = null) {

    // Suppose that one GCS request is 50ms
    // => 20 rps from a single thread.
    // => 200 rps per 10 threads which is way below 1000 rps for a bucket.
    // Queue capacity of 400 will allow for a queue of doubled max rps value.
    // TODO: this settings won't save us from exceeing single object write limit
    // of 1 rps if requests come as different RPC calls and get into different
    // threads.
    // https://cloud.google.com/storage/quotas#objects
    // Task affinity could help us (e.g. we could try scheduling GCS calls in our own
    // executor service with task affinity.
    private val executor = ThreadPoolExecutor(
        2, 10, 60L, TimeUnit.SECONDS, Queues.newArrayBlockingQueue(400),
        GrpcUtil.getThreadFactory("cosmas-grpc-thread-%d", true))
    private val server: Server

    init {
        var builder = ServerBuilder.forPort(port).addService(service)
        if (certChain != null && privateKey != null) {
            Preconditions.checkState(certChain.exists(), "SSL certificate file doesn't exists: %s", certChain)
            Preconditions.checkState(privateKey.exists(), "SSL key file doesn't exists: %s", privateKey)
            builder = builder.useTransportSecurity(certChain, privateKey)
        }
        builder = builder.executor(this.executor)
        this.server = builder.build()
    }

    fun start() {
        this.server.start()
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                this@CosmasServer.stop()
            }
        })
    }

    private fun stop() {
        this.executor.shutdown()
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
            help = "port to listen on (default 9805)") { toInt() }.default { 9805 }
    val certChain: String? by parser.storing("--cert",
            help = "path to SSL cert").default { null }
    val privateKey: String? by parser.storing("--key",
            help = "path to SSL key").default { null }
    val bucket: String? by parser.storing("--bucket",
            help = "GCS bucket where version history will be stored").default { null }
}

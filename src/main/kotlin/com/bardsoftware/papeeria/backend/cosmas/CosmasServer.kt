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

import io.grpc.stub.StreamObserver
import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.bardsoftware.papeeria.backend.cosmas.CosmasGrpc.*
import io.grpc.Server
import io.grpc.ServerBuilder

class CosmasServer {
    private val PORT = 50051
    private val server: Server = ServerBuilder
            .forPort(PORT)
            .addService(CosmasImpl())
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


fun main(args : Array<String>) {
    val server = CosmasServer()
    server.start()
    server.blockUntilShutDown()
}

class CosmasImpl : CosmasImplBase() {
    private val textVersions= arrayOf("ver0", "ver1", "ver2")
    override fun getVersion(request: GetVersionRequest?,
                            responseObserver: StreamObserver<GetVersionResponse>?) {
        val version = request?.version
        val response: GetVersionResponse =
                GetVersionResponse.newBuilder().setText(textVersions[version!!]).build()
        responseObserver?.onNext(response)
        responseObserver?.onCompleted()
    }
}

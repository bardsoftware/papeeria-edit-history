package com.bardsoftware.papeeria.backend.cosmas

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

import com.bardsoftware.papeeria.backend.cosmas.CosmasProto.*
import com.bardsoftware.papeeria.backend.cosmas.CosmasGrpc.*
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit


class CosmasClient(host: String, port: Int) {
    val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext(true)
            .build()
    val blockingStub = newBlockingStub(channel)


    fun getVersion(version: Int) {
        val request: GetVersionRequest = GetVersionRequest.newBuilder().setVersion(version).build()
        val response: GetVersionResponse = blockingStub.getVersion(request)
        print(response.text)
    }

    @Throws(InterruptedException::class)
    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS)
    }
}

fun main(args: Array<String>) {
    val client = CosmasClient("localhost", 50051)
    try {
        client.getVersion(0)
    } finally {
        client.shutdown()
    }
}
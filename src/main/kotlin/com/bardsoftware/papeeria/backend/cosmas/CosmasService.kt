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

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

/**
 * Special class that can work with requests from client
 */
class CosmasService : CosmasGrpc.CosmasImplBase() {

    private val files = mutableMapOf<String, MutableList<ByteString>>()

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        println("Get request for version: ${request.version}")
        val response = CosmasProto.GetVersionResponse.newBuilder()
        val fileVersions = files[request.fileId]
        if (fileVersions != null &&
                request.version < fileVersions.size &&
                request.version >= 0) {
            val file = fileVersions[request.version]
            response.file = file
        } else {
            println("Bad request")
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        println("Get request for create new version")
        addNewVersion(request)
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse.
                newBuilder().
                build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun addNewVersion(request: CosmasProto.CreateVersionRequest) {
        if (files[request.fileId] == null) {
            files[request.fileId] = mutableListOf()
        }
        val fileVersions = files[request.fileId]
        fileVersions?.add(request.file)
    }
}
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
            response.file = fileVersions[request.version]
        } else {
            printErrorInRequest(request)
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun printErrorInRequest(request: CosmasProto.GetVersionRequest) {
        print("This request is bad: ")
        val fileVersions = files[request.fileId]
        when {
            fileVersions == null ->
                println("there is no file in storage with file id ${request.fileId}")
            request.version >= fileVersions.size ->
                println("in storage file has ${fileVersions.size} versions, but you ask for version ${request.version}")
            request.version < 0 ->
                println("you ask for version ${request.version} that is negative")
        }
    }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        println("Get request for create new version")
        addNewVersion(request)
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun addNewVersion(request: CosmasProto.CreateVersionRequest) {
        val fileVersions = files[request.fileId] ?: mutableListOf()
        fileVersions.add(request.file)
        files[request.fileId] = fileVersions
    }
}
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
import io.grpc.Status
import io.grpc.StatusException
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
        synchronized(files) {
            val fileVersions = files[request.fileId]
            val requestStatus = verifyGetVersionRequest(request)
            if (requestStatus.isOk) {
                response.file = fileVersions?.get(request.version)
            } else {
                println("This request is incorrect: " + requestStatus.description)
                responseObserver.onError(StatusException(requestStatus))
            }
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun verifyGetVersionRequest(request: CosmasProto.GetVersionRequest): Status {
        var status: Status = Status.OK
        val fileVersions = files[request.fileId]
        when {
            fileVersions == null ->
                status = Status.INVALID_ARGUMENT.withDescription(
                        "There is no file in storage with file id ${request.fileId}")
            request.version >= fileVersions.size ->
                status = Status.OUT_OF_RANGE.withDescription(
                        "In storage this file has ${fileVersions.size} versions, " +
                                "but you ask for version ${request.version}")
            request.version < 0 ->
                status = Status.OUT_OF_RANGE.withDescription(
                        "You ask for version ${request.version} that is negative")
        }
        return status
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
        synchronized(files) {
            val fileVersions = files[request.fileId] ?: mutableListOf()
            fileVersions.add(request.file)
            files[request.fileId] = fileVersions
        }
    }
}
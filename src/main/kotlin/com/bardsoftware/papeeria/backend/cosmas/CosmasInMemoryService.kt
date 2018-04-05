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
 * Special class that can work with requests from CosmasClient.
 * This realization stores files in RAM.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasInMemoryService : CosmasGrpc.CosmasImplBase() {

    private val files = mutableMapOf<String, MutableList<ByteString>>()

    override fun listOfFileVersions(request: CosmasProto.ListOfFileVersionsRequest,
                                    responseObserver: StreamObserver<CosmasProto.ListOfFileVersionsResponse>) {
        println("Get request for list of versions file # ${request.fileId}")
        val response = CosmasProto.ListOfFileVersionsResponse.newBuilder()
        val fileVersions = this.files[request.fileId]
        if (fileVersions == null) {
            val status = Status.INVALID_ARGUMENT.withDescription(
                    "There is no file in storage with file id ${request.fileId}")
            println(status.description)
            responseObserver.onError(StatusException(status))
            return
        }
        response.addAllVersions((0 until fileVersions.size).toList().map { i -> i.toLong() })
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        println("Get request for version ${request.version} file # ${request.fileId}")
        val response = CosmasProto.GetVersionResponse.newBuilder()
        synchronized(this.files) {
            val fileVersions = this.files[request.fileId]
            val requestStatus = verifyGetVersionRequest(request)
            if (requestStatus.isOk) {
                response.file = fileVersions?.get(request.version.toInt())
                responseObserver.onNext(response.build())
                responseObserver.onCompleted()
            } else {
                println("This request is incorrect: " + requestStatus.description)
                responseObserver.onError(StatusException(requestStatus))
            }
        }
    }

    private fun verifyGetVersionRequest(request: CosmasProto.GetVersionRequest): Status {
        var status: Status = Status.OK
        val fileVersions = this.files[request.fileId]
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
        println("Get request for create new version of file # ${request.fileId}")
        addNewVersion(request)
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun addNewVersion(request: CosmasProto.CreateVersionRequest) {
        synchronized(this.files) {
            val fileVersions = this.files[request.fileId] ?: mutableListOf()
            fileVersions.add(request.file)
            this.files[request.fileId] = fileVersions
        }
    }
}

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

    private val versions = mutableMapOf<String, MutableList<storedStructure>>()

    private class storedStructure(user1: String, text: String, time: Long) {
        val user = user1
        val patch = text
        val timeStamp = time
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        println("Get request for version ${request.version} file # ${request.fileId}")
        val response = CosmasProto.GetVersionResponse.newBuilder()
        synchronized(this.files) {
            val fileVersions = this.files[request.fileId]
            val requestStatus = verifyGetVersionRequest(request)
            if (requestStatus.isOk) {
                response.file = fileVersions?.get(request.version)
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

    private fun verifyGetHistoryOfVersionRequest(request: CosmasProto.GetHistoryOfVersionRequest): Status {
        var status: Status = Status.OK
        val fileVersions = this.versions[request.fileId]
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

    private fun addNewVersion(request: CosmasProto.CreateVersionRequest) {
        synchronized(this.files) {
            val fileVersions = this.files[request.fileId] ?: mutableListOf()
            fileVersions.add(request.file)
            this.files[request.fileId] = fileVersions
        }
    }

    override fun createHistoryOfVersion(request: CosmasProto.CreateHistoryOfVersionRequest,
                                        responseObserver: StreamObserver<CosmasProto.CreateHistoryOfVersionResponse>) {
        println("Get request for create new history of file # ${request.fileId}")
        synchronized(this.versions) {
            val historyOfFile = versions[request.fileId] ?: mutableListOf()
            historyOfFile.add(storedStructure(request.user, request.patch, request.timeStamp))
            versions[request.fileId] = historyOfFile
        }
        val response: CosmasProto.CreateHistoryOfVersionResponse = CosmasProto.CreateHistoryOfVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun getHistoryOfVersion(request: CosmasProto.GetHistoryOfVersionRequest,
                                      responseObserver: StreamObserver<CosmasProto.GetHistoryOfVersionResponse>) {
        println("Get request for history ${request.version} file # ${request.fileId}")
        val response = CosmasProto.GetHistoryOfVersionResponse.newBuilder()
        synchronized(this.versions) {
            val fileHistoryOfVersions = this.versions[request.fileId]
            val requestStatus = verifyGetHistoryOfVersionRequest(request)
            if (requestStatus.isOk) {
                val history = fileHistoryOfVersions?.get(request.version)
                response.user = history?.user
                response.patch = history?.patch
                response.timeStamp = history?.timeStamp ?: 0
                responseObserver.onNext(response.build())
                responseObserver.onCompleted()
            } else {
                println("This request is incorrect: " + requestStatus.description)
                responseObserver.onError(StatusException(requestStatus))
            }
        }
    }
}

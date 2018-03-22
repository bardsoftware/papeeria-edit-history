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

    private val files: HashMap<String, HashMap<String, ArrayList<ByteString>>> = HashMap()
    
    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        println("Get request for version: ${request.version}")
        val response: CosmasProto.GetVersionResponse
        response = if (!checkRequest(request)) {
            println("Bad request")
            CosmasProto.GetVersionResponse.newBuilder().build()
        } else {
            val file = files[request.projectId]!![request.fileId]!![request.version]
            CosmasProto.GetVersionResponse.newBuilder().setFile(file).build()
        }
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun checkRequest(request: CosmasProto.GetVersionRequest): Boolean {
        return files[request.projectId] != null &&
                files[request.projectId]!![request.fileId] != null &&
                request.version >= 0 &&
                request.version < files[request.projectId]!![request.fileId]!!.size
    }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        println("Get request for create new version")
        addNewVersion(request)
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse.newBuilder().build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    private fun addNewVersion(request: CosmasProto.CreateVersionRequest) {
        if (files[request.projectId] == null) {
            files[request.projectId] = HashMap()
        }
        if (files[request.projectId]!![request.fileId] == null) {
            files[request.projectId]!![request.fileId] = ArrayList()
        }
        val fileVersions = files[request.projectId]!![request.fileId]
        fileVersions?.add(request.file)
    }
}
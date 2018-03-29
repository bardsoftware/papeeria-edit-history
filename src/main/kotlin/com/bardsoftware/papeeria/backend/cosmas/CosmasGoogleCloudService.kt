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

import com.google.cloud.storage.*
import io.grpc.stub.StreamObserver
import com.google.cloud.storage.Acl.User
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.util.Arrays
import java.util.ArrayList

class CosmasGoogleCloudService(private val bucketName: String) : CosmasGrpc.CosmasImplBase() {

    private val storage: Storage = StorageOptions.getDefaultInstance().service

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        val blobInfo = storage.create(
                BlobInfo.newBuilder(bucketName, request.fileId)
                        // Modify access list to allow all users with link to read file
                        .setAcl(ArrayList(Arrays.asList(Acl.of(User.ofAllUsers(), Acl.Role.READER))))
                        .build(),
                request.file.toByteArray())
        println(blobInfo.mediaLink)
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()

    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        val blob = storage.get(BlobInfo.newBuilder(bucketName, request.fileId).build().blobId)
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if(blob != null) {
            response.file = ByteString.copyFrom(blob.getContent())
        } else {
            val requestStatus = Status.NOT_FOUND.withDescription("There is no such file in storage")
            println(println("This request is incorrect: " + requestStatus.description))
            responseObserver.onError(StatusException(requestStatus))
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

}
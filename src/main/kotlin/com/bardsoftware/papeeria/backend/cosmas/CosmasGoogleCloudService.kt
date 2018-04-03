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
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException
import java.util.Arrays
import java.util.ArrayList

/**
 * Special class that can work with requests from CosmasClient
 * This realization stores files in Google Cloud Storage.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudService(private val bucketName: String) : CosmasGrpc.CosmasImplBase() {

    private var storage: Storage = StorageOptions.getDefaultInstance().service

    private constructor(bucketName: String, storage: Storage) : this(bucketName) {
        this.storage = storage
    }

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        println("Get request for create new version of file # ${request.fileId}")
        try {
            this.storage.create(
                    BlobInfo.newBuilder(this.bucketName, request.fileId).build(),
                    request.file.toByteArray())
        } catch (e: StorageException) {
            println("StorageException happened: ${e.message}")
            responseObserver.onError(e)
            return
        }
        val response: CosmasProto.CreateVersionResponse = CosmasProto.CreateVersionResponse
                .newBuilder()
                .build()
        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

    override fun getVersion(request: CosmasProto.GetVersionRequest,
                            responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {
        println("Get request for version ${request.version} file # ${request.fileId}")
        val blob: Blob? = try {
            this.storage.get(BlobInfo.newBuilder(this.bucketName, request.fileId).build().blobId)
        } catch (e: StorageException) {
            println("StorageException happened: ${e.message}")
            responseObserver.onError(e)
            return
        }
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if (blob != null) {
            response.file = ByteString.copyFrom(blob.getContent())
            responseObserver.onNext(response.build())
            responseObserver.onCompleted()
        } else {
            val requestStatus = Status.NOT_FOUND.withDescription("There is no such file in storage")
            println("This request is incorrect: " + requestStatus.description)
            responseObserver.onError(StatusException(requestStatus))
        }
    }

    fun deleteFile(fileId: String) {
        println("Delete file # $fileId")
        try {
            this.storage.delete(BlobInfo.newBuilder(this.bucketName, fileId).build().blobId)
        } catch (e: StorageException) {
            println("Deleting file failed: ${e.message}")
        }
    }

    companion object {
        fun getServiceForTests(): CosmasGoogleCloudService {
            return CosmasGoogleCloudService("papeeria-interns-cosmas",
                    LocalStorageHelper.getOptions().service)
        }
    }
}
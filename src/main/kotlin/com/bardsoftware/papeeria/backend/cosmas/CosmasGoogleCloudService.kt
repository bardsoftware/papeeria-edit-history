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

import com.google.api.gax.paging.Page
import com.google.cloud.storage.*
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import io.grpc.Status
import io.grpc.StatusException

/**
 * Special class that can work with requests from CosmasClient
 * This realization stores files in Google Cloud Storage.
 *
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasGoogleCloudService(private val bucketName: String,
                               private val storage: Storage = StorageOptions.getDefaultInstance().service) : CosmasGrpc.CosmasImplBase() {

    override fun createVersion(request: CosmasProto.CreateVersionRequest,
                               responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        println("Get request for create new version of file # ${request.fileId}")
        try {
            val blob = this.storage.create(
                    BlobInfo.newBuilder(this.bucketName, request.fileId).build(),
                    request.file.toByteArray())
            println("Generation of created file: ${blob.generation}")
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
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
            this.storage.get(BlobInfo.newBuilder(this.bucketName, request.fileId).build().blobId,
                    Storage.BlobGetOption.generationMatch(request.version))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        val response = CosmasProto.GetVersionResponse.newBuilder()
        if (blob == null) {
            val requestStatus = Status.NOT_FOUND.withDescription(
                    "There is no such file or file version in storage")
            println("This request is incorrect: " + requestStatus.description)
            responseObserver.onError(StatusException(requestStatus))
            return
        }
        println("Generation: ${blob.generation}")
        response.file = ByteString.copyFrom(blob.getContent())
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    override fun fileVersionList(request: CosmasProto.FileVersionListRequest,
                                 responseObserver: StreamObserver<CosmasProto.FileVersionListResponse>) {
        println("Get request for list of versions file # ${request.fileId}")
        val response = CosmasProto.FileVersionListResponse.newBuilder()
        val blobs: Page<Blob> = try {
            this.storage.list(this.bucketName, Storage.BlobListOption.versions(true),
                    Storage.BlobListOption.prefix(request.fileId))
        } catch (e: StorageException) {
            handleStorageException(e, responseObserver)
            return
        }
        blobs.iterateAll().forEach {
            response.addVersions(it.generation)
        }
        if (response.versionsList.isEmpty()) {
            val status = Status.INVALID_ARGUMENT.withDescription(
                    "There is no file in storage with file id ${request.fileId}")
            println(status.description)
            responseObserver.onError(StatusException(status))
            return
        }
        responseObserver.onNext(response.build())
        responseObserver.onCompleted()
    }

    private fun handleStorageException(e: StorageException, responseObserver: StreamObserver<*>) {
        println("StorageException happened: ${e.message}")
        responseObserver.onError(e)
    }

    fun deleteFile(fileId: String) {
        println("Delete file # $fileId")
        try {
            this.storage.delete(BlobInfo.newBuilder(this.bucketName, fileId).build().blobId)
        } catch (e: StorageException) {
            println("Deleting file failed: ${e.message}")
        }
    }
}
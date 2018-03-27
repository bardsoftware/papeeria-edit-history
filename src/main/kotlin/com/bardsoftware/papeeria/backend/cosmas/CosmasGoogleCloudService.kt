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

import com.google.cloud.Timestamp
import com.google.cloud.datastore.*
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import javax.swing.UIManager.put
import com.google.cloud.datastore.TransactionExceptionHandler.build
import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import sun.security.rsa.RSAPrivateCrtKeyImpl.newKey
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.Storage


class CosmasGoogleCloudService(private val bucketName: String): CosmasGrpc.CosmasImplBase() {

    val datastore = DatastoreOptions.getDefaultInstance().getService()

    //val keyFactory = datastore.newKeyFactory().setKind("File")

    override fun getVersion(request: CosmasProto.GetVersionRequest, responseObserver: StreamObserver<CosmasProto.GetVersionResponse>) {

    }


    override fun createVersion(request: CosmasProto.CreateVersionRequest, responseObserver: StreamObserver<CosmasProto.CreateVersionResponse>) {
        val key = datastore.allocateId(keyFactory.newKey())
        val blob: Blob = Blob(request.file)
        val task = Entity.newBuilder(key)
                .set("description", Blob(request.file))
                .set("created", Timestamp.now())
                .build()
        datastore.put(task)
    }

}
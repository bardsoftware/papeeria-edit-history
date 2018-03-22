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

import org.junit.Assert.*
import org.junit.Test
import io.grpc.internal.testing.StreamRecorder

/**
 * This is simple test to check that CosmasImpl class returns correct response.
 * @author Aleksandr Fedotov (iisuslik43)
 */
class CosmasImplTest {
    @Test
    fun itGivesCorrectVersion() {
        val service = CosmasImpl()
        val request: CosmasProto.GetVersionRequest =
                CosmasProto.GetVersionRequest.newBuilder().setVersion(1).build()
        val recorder: StreamRecorder<CosmasProto.GetVersionResponse> = StreamRecorder.create()
        service.getVersion(request, recorder)
        assertEquals("ver1", recorder.values[0].text)
    }
}
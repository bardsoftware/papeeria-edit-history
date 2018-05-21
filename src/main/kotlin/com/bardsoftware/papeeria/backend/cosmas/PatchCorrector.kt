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
import java.util.LinkedList
import name.fraser.neil.plaintext.diff_match_patch

object PatchCorrector {

    fun reversePatch(patchList: LinkedList<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        val dmp = diff_match_patch()
        val newText = dmp.patch_apply(patchList, text)
        if ((newText[1] as BooleanArray).any { !it }) {
            throw DeletePatchException("Failure in patch apply")
        }
        return dmp.patch_make(newText[0] as String, text)
    }

    fun deletePatch(deletedPatch: LinkedList<diff_match_patch.Patch>,
                    nextPatches: List<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        val dmp = diff_match_patch()
        var textVersion = dmp.patch_apply(deletedPatch, text)[0] as String
        val applyList = LinkedList<diff_match_patch.Patch>()
        var reversePatches = LinkedList<diff_match_patch.Patch>()
        reversePatches.addAll(reversePatch(deletedPatch, text))
        for (patch in nextPatches) {
            applyList.clear()
            applyList.add(patch)
            val nextVersion = dmp.patch_apply(applyList, textVersion)
            val nextVersionWithoutPatch = dmp.patch_apply(reversePatches, nextVersion[0] as String)
            if ((nextVersion[1] as BooleanArray).any { !it } || (nextVersionWithoutPatch[1] as BooleanArray).any { !it }) {
                throw DeletePatchException("Failure in patch apply")
            }
            reversePatches = dmp.patch_make(nextVersion[0] as String, nextVersionWithoutPatch[0] as String)
            textVersion = nextVersion[0] as String
        }
        return reversePatches
    }

    public class DeletePatchException(message: String) : Throwable(message)
}
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
    private val dmp = diff_match_patch()

    private fun textToPatches(patchList: List<CosmasProto.Patch>) : LinkedList<diff_match_patch.Patch> {
        val newPatchList = LinkedList<diff_match_patch.Patch>()
        for (patch in patchList) {
            newPatchList.addAll(LinkedList(dmp.patch_fromText(patch.text)))
        }
        return newPatchList
    }

    fun applyPatch(patchList: LinkedList<diff_match_patch.Patch>, text: String) : String {
        val newText =  dmp.patch_apply(patchList, text)
        if ((newText[1] as BooleanArray).any { !it }) {
            throw ApplyPatchException("Failure in patch apply")
        }
        return newText[0] as String
    }

    fun applyPatch(patchList: List<CosmasProto.Patch>, text: String) : String {
        val newText = dmp.patch_apply(textToPatches(patchList), text)
        if ((newText[1] as BooleanArray).any { !it }) {
            throw ApplyPatchException("Failure in patch apply")
        }
        return newText[0] as String
    }

    fun reversePatch(patchList: LinkedList<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        val newText = dmp.patch_apply(patchList, text)
        if ((newText[1] as BooleanArray).any { !it }) {
            throw ApplyPatchException("Failure in patch apply")
        }
        return dmp.patch_make(newText[0] as String, text)
    }

    fun deletePatch(deleteCandidate: CosmasProto.Patch,
                    nextPatches: List<CosmasProto.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        return deletePatch(LinkedList(dmp.patch_fromText(deleteCandidate.text)), textToPatches(nextPatches), text)
    }

    fun deletePatch(deleteCandidate: LinkedList<diff_match_patch.Patch>,
                    nextPatches: List<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        var textVersion = dmp.patch_apply(deleteCandidate, text)[0] as String
        var reversePatches = LinkedList<diff_match_patch.Patch>()
        reversePatches.addAll(reversePatch(deleteCandidate, text))
        for (patch in nextPatches) {
            val applyList = LinkedList<diff_match_patch.Patch>()
            applyList.add(patch)
            val nextVersion = dmp.patch_apply(applyList, textVersion)
            val nextVersionWithoutPatch = dmp.patch_apply(reversePatches, nextVersion[0] as String)
            if ((nextVersion[1] as BooleanArray).any { !it } || (nextVersionWithoutPatch[1] as BooleanArray).any { !it }) {
                throw ApplyPatchException("Failure in patch apply")
            }
            reversePatches = dmp.patch_make(nextVersion[0] as String, nextVersionWithoutPatch[0] as String)
            textVersion = nextVersion[0] as String
        }
        return reversePatches
    }

    public class ApplyPatchException(message: String) : Throwable(message)
}
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
    private fun getReverseOperation(operation: diff_match_patch.Operation): diff_match_patch.Operation {
        return when (operation) {
            diff_match_patch.Operation.DELETE -> diff_match_patch.Operation.INSERT
            diff_match_patch.Operation.INSERT -> diff_match_patch.Operation.DELETE
            else -> {
                diff_match_patch.Operation.EQUAL
            }
        }
    }

    fun reversePatch(patchList: LinkedList<diff_match_patch.Patch>): LinkedList<diff_match_patch.Patch> {
        val reversePatchList = LinkedList<diff_match_patch.Patch>()
        for (patch in patchList) {
            val newPatch = diff_match_patch.Patch()
            newPatch.length1 = patch.length2
            newPatch.length2 = patch.length1
            newPatch.start1 = patch.start2
            newPatch.start2 = patch.start1
            for (diffs in patch.diffs) {
                newPatch.diffs.add(diff_match_patch.Diff(getReverseOperation(diffs.operation), diffs.text))
            }
            reversePatchList.add(newPatch)
        }
        return reversePatchList
    }

    fun deletePatch(deletedPatch: LinkedList<diff_match_patch.Patch>,
                    nextPatches: List<diff_match_patch.Patch>, text: String): LinkedList<diff_match_patch.Patch> {
        val dmp = diff_match_patch()
        var textVersion = text
        val applyList = LinkedList<diff_match_patch.Patch>()
        var reversePatches = LinkedList<diff_match_patch.Patch>()
        reversePatches.addAll(reversePatch(deletedPatch))
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
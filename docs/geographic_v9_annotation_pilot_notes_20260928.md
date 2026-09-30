# GeoSnap v9: stopped 11-image annotation pilot

The user voluntarily stopped after 11 of the randomized 20 images. All 11 form submissions and their free-text notes were saved before any outcome group was revealed. The append-only source is `data/evaluation/geographic_v9_20260928/annotations/quick20_rater_reviewer1.jsonl`; its sealed label hash and descriptive outcome join are in `data/evaluation/geographic_v9_20260928/annotations/partial_pilot_11_395eae3f7f77.json`. There is one rater and no agreement estimate. The subset is too small and incomplete for population prevalence or model training.

The scores were two at level 1, four at level 2 and five at level 3. Both level-1 images were baseline retrieval failures, but a retrieval failure was also rated level 3. A good-looking image can therefore still defeat retrieval, even in this tiny pilot. Nine of eleven forms requested an additional view, including all three baseline-correct images. Under the current prompt, “another photo could help” is a permissive recommendation, **not** an abstention decision or evidence that the current photo is unusable.

The free-text comments add information that the checkboxes missed:

- `inside_vehicle` was explicitly used as context, not necessarily a defect. Several images marked this way scored 3. A future annotation schema should move this to a separate capture-context field and ask whether the vehicle/window actually obstructs useful details. Do not reinterpret or edit the original labels.
- One view was reported upside down; the pilot taxonomy has no orientation/corruption distinction for this. Add an orientation label for a future version.
- Dirty glass, windshield-edge blur, headlight glare and motion blur were described separately. A future schema should separate glass obstruction and glare from general under/overexposure and focus.
- Comments on otherwise usable scenes repeatedly proposed another road direction or a second photograph. The product should distinguish **usable but ambiguous** from **poor input**, and it should not translate every “another view might help” into a forced retake.
- One score-3 image was described as visually good but with uncertain location evidence. The next form should clarify the boundary between technical quality and geolocation utility with examples and perhaps a separate ambiguity question.

No current evidence supports training a geolocatability classifier, selecting a retake threshold, or estimating how many of the 1,184 failures poor input caused. The technical feature cache is an image-only baseline awaiting a larger, independently rated set.

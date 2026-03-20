# Unranked

This folder captures what is known locally about scanned FIDs that did not end up
in the committed `>= 0.99` dataset.

Important limitation:

- The crawler did **not** persist every scanned-below-threshold FID.
- Because of that, a full offline `unranked FID list` cannot be reconstructed
  without calling the API again or changing the crawler to persist every scanned
  FID during collection.

What is included here instead:

- exact scan totals from the local cache
- exact ranked-match totals
- exact non-ranked count implied by the scan
- shard completion state and resume token for the paused shard

The canonical file in this folder is `fids-scanned.partial.json`.

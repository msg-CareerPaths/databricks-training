# OPT-2. Structured Streaming Deep-Dive [3 hours]

_Optional · Exam domains 2 & 3_

**Goal:** go deeper on the streaming concepts the telematics source touches — triggers
(`availableNow` vs continuous), checkpoints & exactly-once, **watermarks**, and stateful
de-duplication of late / out-of-order events.

## Mandatory Materials:
**Reading:**
 - [Studybook M3 — Silver Transform](https://github.com/msg-CareerPaths/databricks-training/blob/main/docs/studybook/M3_silver_transform.md) (watermark section)
 - [Structured Streaming](https://docs.databricks.com/en/structured-streaming/index.html) · [Watermarks](https://docs.databricks.com/en/structured-streaming/watermarks.html)

## Insurance Lakehouse:
 > Re-run telematics ingestion with different watermark thresholds and observe how many late
 > events are kept vs dropped. Explain the trade-off between state size and completeness, and why
 > `trigger(availableNow=True)` is the budget-friendly choice on Free Edition.

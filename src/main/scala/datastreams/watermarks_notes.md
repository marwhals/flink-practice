# Watermarks

---

## Event time
- The moment when the record was generated
  - Set by the data generation system
  - Flink has APIs to extract the event time
  - Different processing time i.e the time which the record arrives at Flink

---

## Watermarks

- Mechanism to discard late data
  - Events may arrive much later than they were created
  - We can set a marker to ignore events older than a threshold (watermark)
  - As events arrive at Flink, the marker is automatically (or manually) updated
- Example
  - We emit a watermark with every incoming element with event time > current max
  - Logic: Ignore all data older than the watermark minus 2 seconds
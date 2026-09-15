# CKD Production - Smoke Test

Run: 2026-09-14T19:47:36

**11/11 checks passed**

| Check | Result | Detail |
|---|---|---|
| Medallion row counts | PASS | bronze_patient/lab/notes/silver/gold = ['2000', '12200', '1018', '2000', '2000'] |
| Inpatient labs excluded in silver | PASS | 200 inpatient events tagged (excluded from silver) |
| Gold risk flags | PASS | ckd/undoc/mis/hirisk/rapid = ['1524', '518', '527', '238', '160'] |
| Governance: PK/FK constraints | PASS | [['FOREIGN KEY', '2'], ['PRIMARY KEY', '3']] |
| Governance: PHI mask on MRN | PASS | masked value = MRN-***-**** |
| AI note extraction | PASS | 1018 notes, 841 CKD mentions |
| ML scores | PASS | 2000 scored, 1994 agree, conf 0.977 |
| Metric view MEASURE query | PASS | metric view = ['2000', '1524', '518', '238'] |
| Analytics view | PASS | 2000 rows, 2000 with model suggestion |
| App write-back path (insert/read) | PASS | insert+read OK |
| App running state | PASS | ACTIVE |

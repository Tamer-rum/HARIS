# External HARIS diagnostics

These manual Nokia diagnostics are deliberately outside normal test discovery.
They are not unit or regression tests and must only run in the explicit
external-integration environment:

```powershell
$env:HARIS_RUNTIME_ENV = "external_integration"
$env:HARIS_ALLOW_EXTERNAL_TESTS = "true"
```

Run one only as a module, for example
`python -m external.manual_live_congestion`. They are never invoked by
`python run_offline_tests.py`.

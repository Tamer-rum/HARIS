# HARIS Current Implementation Alignment

This repository implements the submitted GSMA MENA Ignite Hackathon 2026 storyboard; the submitted deck itself is not modified here.

- Seven capability groups are represented: Congestion Insights, Device Status, Location Retrieval, Geofencing, Quality on Demand, Network Slicing, and Number Verification / SIM Swap.
- Number Verification and SIM Swap are used only by WARDEN-owned Trusted Dispatch for privileged field intervention. Routine network remediation does not invoke identity checks.
- The audit history is a tamper-evident append-only SHA-256 chain. It is not described as immutable or digitally signed storage.
- 5G/LTE bearer switching and microwave/fibre steering remain operator-domain roadmap capabilities; this project does not claim to execute either.
- Nokia simulator validation and sandbox limits are documented in `README.md`.

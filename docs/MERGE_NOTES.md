# Merge notes — `merge/combined`

This branch is Tamer's HARIS (`main` at `dbb62a8`) plus the parts of the team's
parallel prototype that it did not already have. Nothing in the durable core,
the dispatch security flow or the Nokia clients was changed.

## What was added

| change | files | why |
|---|---|---|
| Local AI advisor (Ollama) as the last tier of the planner's advisory chain | `agents.py` (`LocalChatModel`, `ReasoningRouter`), `config.py` (`LOCAL_LLM_*`), `.env.example` | A storm can take the backhaul down; the advisor keeps working on the laptop. Loopback URLs only, bounded exactly like Gemini/Groq, and the only advisor the isolated fixture demo may consult. |
| Console line for the local advisor | `app.py` (fixture demo panel) | Judges see which model answered and that no internet was used. |
| Tests | `test_local_llm.py` (8 tests) | Loopback-only URLs, chain order, hosted-first, local-only in the isolated demo, hallucinated IDs rejected, TEST runtime never builds it. |
| Storm map | `app.py` (`render_storm_map`), `test_storm_map.py` (3 tests) | Before/after geographic map in Autonomous Operations and Network Intelligence: towers coloured by the cycle's congestion evidence, the storm-impact circle, tier-1 devices and a green ring on protected ones. Site positions are labelled SIMULATED demo geography. |
| One-click launcher | `START-HARIS.bat`, `STOP-HARIS.bat`, `.gitattributes`, `assets/haris.ico` | Creates `.venv`, installs requirements, copies `.env.example`, detects Ollama, pre-loads the model, opens the console. |
| Screenshot capture | `scripts/capture_screens.py` | Regenerates the images used in the README and the deck. |
| Submission texts | `docs/SUBMISSION.md` | Demo description, API synopsis, commercial value, business impact, honest scope and the 3-minute video script, written against this code. |
| README | local AI section, one-click setup, test count | |

Offline suite on this branch: **690 passed, 0 failed, OFFLINE_SAFE** (679 before + 11 new).

## Before submitting — deployed app

On 12 Sep 2026 the deployed Streamlit app showed `SYSTEM STATUS: NOT_READY`,
`NOKIA NaC: UNAVAILABLE`, `MODE: UNAVAILABLE` and no completed cycle, although
the Render backend reported healthy. A judge opening it cold sees an empty
dashboard. Please check, on Render:

1. `NAC_MODE` and `NAC_API_TOKEN` are set, and `NOKIA_OBSERVATION_ENABLED=true`
   if the header should show live Nokia evidence.
2. `HARIS_OPERATIONAL_API_TOKEN` on Render matches `HARIS_BACKEND_API_TOKEN`
   in the Streamlit secrets.
3. Run one autonomous cycle and one Real Nokia read check right before the
   submission so the dashboard is not empty, and keep the Render service warm
   (the free tier sleeps; the first request took ~84 s).

The local advisor needs Ollama on the same machine, so it stays off on
Render/Streamlit Cloud — nothing to configure there.

## Security

The zip of this repo that was shared inside the team contained a real `.env`
(Nokia token, Supabase URL and key). It is not in git (`.gitignore` covers it;
`GET /repos/Tamer-rum/HARIS/contents/.env` returns 404), but the zip should not
be forwarded, and all keys should be rotated after the hackathon.

"""Transparent short-horizon risk forecasting over available HARIS evidence.

This is deliberately a risk forecasting model, not a trained ML model.  It
uses categorical Nokia congestion history plus the dust advisory and never
converts Nokia categories into fabricated numeric KPIs.
"""
from __future__ import annotations

from collections import defaultdict, deque
from typing import Deque, Dict, Iterable, List

from pydantic import BaseModel, Field

from nokia_clients import CongestionReading


class PredictionResult(BaseModel):
    horizon_minutes: int = 15
    predicted_risk_level: str
    confidence: float = Field(ge=0, le=1)
    degradation_probability: float = Field(ge=0, le=1)
    contributing_factors: List[str]
    affected_cells: List[str]
    evidence_source: str
    model_type: str = "categorical risk forecasting model"


class RiskForecaster:
    """Small state-transition forecaster suitable for bounded control loops."""

    _rank = {"None": 0, "Low": 1, "Medium": 2, "High": 3}

    def __init__(self, history_size: int = 6):
        self.history: Dict[str, Deque[int]] = defaultdict(lambda: deque(maxlen=history_size))

    def predict(self, readings: Iterable[CongestionReading], dust_advisory: bool, environmental_source: str = "FIXTURE") -> PredictionResult:
        readings = list(readings)
        factors: List[str] = []
        affected: List[str] = []
        probabilities: List[float] = []
        evidence_count = 0

        for reading in readings:
            rank = self._rank.get(reading.congestion_level)
            if rank is None:
                continue
            previous = list(self.history[reading.cell_id])
            self.history[reading.cell_id].append(rank)
            evidence_count += 1
            # Current categorical severity is evidence, not a fabricated KPI.
            probability = (0.10, 0.25, 0.55, 0.80)[rank]
            if previous and rank > previous[-1]:
                probability += 0.15
                factors.append(f"{reading.cell_id} categorical congestion is rising")
            if rank >= 2:
                affected.append(reading.cell_id)
            probabilities.append(min(0.95, probability))

        if dust_advisory:
            probabilities = [min(0.95, value + 0.15) for value in probabilities] or [0.25]
            factors.append("dust advisory is active")
        if not factors and readings:
            factors.append("current categorical congestion is stable")
        if not readings:
            factors.append("no current congestion evidence available")

        probability = max(probabilities, default=0.10)
        confidence = min(0.90, 0.35 + 0.10 * evidence_count + (0.10 if dust_advisory else 0.0))
        if not readings:
            confidence = 0.20
        level = "High" if probability >= 0.70 else "Medium" if probability >= 0.40 else "Low"
        return PredictionResult(
            predicted_risk_level=level,
            confidence=confidence,
            degradation_probability=probability,
            contributing_factors=factors[:4],
            affected_cells=sorted(set(affected)),
            evidence_source=f"Nokia categorical congestion history + HARIS dust advisory ({environmental_source})",
        )

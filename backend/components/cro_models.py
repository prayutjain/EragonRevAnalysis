# cro_models.py - CRO-specific models and type definitions
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class ChartOptions:
    """Options for chart generation"""
    title: Optional[str] = None
    xAxis: str = "label"
    yAxis: str = "value"
    yFormat: Optional[str] = None
    label: Optional[str] = None


@dataclass
class UIBlock:
    """UI Block structure"""
    type: str  # headline, markdown, table, chart, kpis
    content: Optional[str] = None
    text: Optional[str] = None
    columns: Optional[List[str]] = None
    rows: Optional[List[List[Any]]] = None
    id: Optional[str] = None
    chartType: Optional[str] = None
    data: Optional[Any] = None
    xAxis: Optional[str] = None
    yAxis: Optional[str] = None
    yFormat: Optional[str] = None
    values: Optional[Dict[str, Any]] = None


@dataclass
class Visualization:
    """Visualization structure"""
    type: str  # html, chart, etc.
    id: str
    title: str
    content: str


@dataclass
class ConfidenceIndicator:
    """Confidence indicator structure"""
    emoji: str
    text: str
    color: str


@dataclass
class AnswerData:
    """Structured answer data"""
    answer: str
    blocks: List[Dict[str, Any]]
    visualizations: List[Dict[str, Any]]
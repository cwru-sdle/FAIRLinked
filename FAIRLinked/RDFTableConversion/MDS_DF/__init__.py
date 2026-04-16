"""
FAIRLinked.MDS_DF Module

This module provides tools for tracking scientific analysis, managing 
metadata using RDF ontologies, and generating JSON-LD provenance graphs 
to ensure research data is Findable, Accessible, Interoperable, and Reusable (FAIR).

Main Components:
----------------
- **MatDatSciDf**: The core semantic-aware DataFrame that integrates 
  tabular data with ontology-based metadata.
- **AnalysisTracker**: A context manager and decorator system for 
  capturing function execution provenance and file system events.
- **AnalysisGroup**: A management class for aggregating multiple 
  analysis runs into a consolidated master graph.
- **Metadata**: Handles the lifecycle of metadata templates and 
  ontology mappings.
- **DataRelationsDict**: Manages semantic relationships and links 
  between disparate data entities.

Example:
    >>> from fairlinked import AnalysisTracker
    >>> tracker = AnalysisTracker(proj_name="MyExperiment", home_path="./results")
    >>> @tracker.track
    >>> def my_analysis_step(data):
    >>>     return data * 2
"""

from .main import MatDatSciDf
from .metadata_manager import Metadata
from .data_relations_manager import DataRelationsDict
from .analysis_tracker import AnalysisGroup, AnalysisTracker

__all__ = ["MatDatSciDf", "Metadata", "DataRelationsDict", "AnalysisGroup", "AnalysisTracker"]
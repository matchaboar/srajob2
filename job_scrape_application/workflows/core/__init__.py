"""Core workflow infrastructure with dependency injection.

This module provides the foundation for running workflows in both
production and test environments with the same code paths.

Usage:
    # Production mode (default)
    from job_scrape_application.workflows.core import DependencyContainer
    deps = DependencyContainer.production()

    # Testing mode
    deps = DependencyContainer.testing(
        query_fixtures={"router:listSites": [...]},
        captured_mutations=captured_list,
    )

    # Capturing mode (for generating fixtures)
    deps = DependencyContainer.capturing(
        captured_scrapes=scrape_list,
    )

Protocols:
    - ConvexClientProtocol: Interface for Convex database operations
    - QueueServiceProtocol: Interface for DBOS queue operations
    - SettingsProtocol: Interface for application settings
    - SpiderClientProtocol: Interface for SpiderCloud client

Mock implementations:
    - MockConvexFunctions: Returns fixture data for queries/mutations
    - MockQueueService: In-memory queue implementation
    - MockSettings: Configurable settings mock
    - MockSpiderClient: Returns fixture data for scrapes
"""

from .dependencies import DependencyContainer
from .mock_clients import (
    CapturingSpiderClient,
    MockConvexFunctions,
    MockQueueService,
    MockSettings,
    MockSpiderClient,
    MockSpiderClientFactory,
)
from .protocols import (
    ConvexClientProtocol,
    ConvexFunctions,
    QueueServiceProtocol,
    RuntimeConfigProtocol,
    SettingsProtocol,
    SpiderClientProtocol,
)
from .test_helpers import CapturedConvexData, SpiderFixture, WorkflowTestHelper
from .listing_workflow import ListingExtractionTrace, ListingWorkflowModule

__all__ = [
    # Main container
    "DependencyContainer",
    # Protocols
    "ConvexClientProtocol",
    "ConvexFunctions",
    "QueueServiceProtocol",
    "RuntimeConfigProtocol",
    "SettingsProtocol",
    "SpiderClientProtocol",
    # Mock implementations
    "CapturingSpiderClient",
    "MockConvexFunctions",
    "MockQueueService",
    "MockSettings",
    "MockSpiderClient",
    "MockSpiderClientFactory",
    # Test helpers
    "CapturedConvexData",
    "SpiderFixture",
    "WorkflowTestHelper",
    # Listing workflow
    "ListingExtractionTrace",
    "ListingWorkflowModule",
]

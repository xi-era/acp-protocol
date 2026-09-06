"""pytest path setup: expose the conformance suite as a top-level `suite` module."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "conformance"))

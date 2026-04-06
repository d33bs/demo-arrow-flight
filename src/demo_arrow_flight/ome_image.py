"""Utilities for creating a small demo OME-Arrow image payload."""

from __future__ import annotations

import numpy as np
import ome_arrow
import pyarrow as pa


def build_demo_image(height: int = 96, width: int = 128) -> np.ndarray:
    """Build a deterministic 2D uint16 image for demos/tests."""
    y = np.linspace(0, 1, height, dtype=np.float32)[:, None]
    x = np.linspace(0, 1, width, dtype=np.float32)[None, :]
    image = (x * 0.65 + y * 0.35) * 65535
    return image.astype(np.uint16)


def build_demo_ome_arrow(image: np.ndarray | None = None) -> pa.StructScalar:
    """Create an OME-Arrow struct scalar from a numpy image."""
    if image is None:
        image = build_demo_image()

    return ome_arrow.from_numpy(
        image,
        dim_order="YX",
        name="demo-flight-image",
        image_type="image",
        physical_size_x=0.25,
        physical_size_y=0.25,
        physical_size_unit="µm",
    )

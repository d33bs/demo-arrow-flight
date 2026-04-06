from __future__ import annotations

import numpy as np
import ome_arrow

from demo_arrow_flight.ome_image import build_demo_image, build_demo_ome_arrow


def test_build_demo_image_shape_and_dtype() -> None:
    image = build_demo_image(height=32, width=48)
    assert image.shape == (32, 48)
    assert image.dtype == np.uint16


def test_build_demo_ome_arrow_roundtrips_to_numpy() -> None:
    image = build_demo_image(height=24, width=36)
    scalar = build_demo_ome_arrow(image)

    restored = np.squeeze(ome_arrow.to_numpy(scalar))
    np.testing.assert_array_equal(restored, image)

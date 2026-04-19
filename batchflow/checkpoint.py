import json
import os
from datetime import datetime
from typing import Any, Dict, Optional


class Checkpoint:
    """Manages pipeline checkpointing for resumable batch processing."""

    def __init__(self, checkpoint_dir: str = ".batchflow_checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)

    def _checkpoint_path(self, pipeline_id: str) -> str:
        return os.path.join(self.checkpoint_dir, f"{pipeline_id}.json")

    def save(self, pipeline_id: str, batch_index: int, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Save a checkpoint for the given pipeline at the specified batch index."""
        data = {
            "pipeline_id": pipeline_id,
            "batch_index": batch_index,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {},
        }
        path = self._checkpoint_path(pipeline_id)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Load the latest checkpoint for the given pipeline. Returns None if not found."""
        path = self._checkpoint_path(pipeline_id)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)

    def clear(self, pipeline_id: str) -> None:
        """Remove the checkpoint for the given pipeline."""
        path = self._checkpoint_path(pipeline_id)
        if os.path.exists(path):
            os.remove(path)

    def exists(self, pipeline_id: str) -> bool:
        """Check whether a checkpoint exists for the given pipeline."""
        return os.path.exists(self._checkpoint_path(pipeline_id))

# batchflow

A lightweight Python library for building resumable batch data processing pipelines with checkpointing.

---

## Installation

```bash
pip install batchflow
```

Or install from source:

```bash
git clone https://github.com/yourname/batchflow.git
cd batchflow && pip install -e .
```

---

## Usage

```python
from batchflow import Pipeline, checkpoint

@checkpoint("processed_users")
def process_users(batch):
    return [transform(record) for record in batch]

pipeline = Pipeline(source="data/users.jsonl", batch_size=500)
pipeline.add_step(process_users)
pipeline.run()
```

If the pipeline is interrupted, re-running it will automatically **skip completed batches** and resume from the last checkpoint.

```python
# Resume a previously interrupted pipeline
pipeline.run(resume=True)
```

Checkpoints are stored locally (default: `.batchflow_cache/`) and can be configured:

```python
pipeline = Pipeline(
    source="data/users.jsonl",
    batch_size=500,
    checkpoint_dir="/tmp/my_checkpoints"
)
```

---

## Features

- ✅ Resumable pipelines with automatic checkpointing
- ✅ Simple decorator-based step definitions
- ✅ Pluggable data sources (files, databases, APIs)
- ✅ Minimal dependencies

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.
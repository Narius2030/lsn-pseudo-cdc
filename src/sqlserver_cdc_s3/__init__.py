"""SQL Server CDC to S3 pipeline."""


def run_pipeline(config_path: str):
    from .pipeline import run_pipeline as _run_pipeline

    return _run_pipeline(config_path)


__all__ = ["run_pipeline"]

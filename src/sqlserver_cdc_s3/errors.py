"""Custom exceptions for the CDC pipeline."""


class CDCToS3Error(Exception):
    """Base exception for the pipeline."""


class ConfigurationError(CDCToS3Error):
    """Raised when configuration is invalid."""


class MetadataError(CDCToS3Error):
    """Raised when CDC metadata cannot be read or is inconsistent."""


class VersionCompatibilityError(CDCToS3Error):
    """Raised when the SQL Server build is incompatible with strict CDC rules."""


class CDCGapError(CDCToS3Error):
    """Raised when the saved bookmark is older than the CDC retention window."""


class TransformError(CDCToS3Error):
    """Raised when change rows cannot be converted into Debezium-style events."""


class StateError(CDCToS3Error):
    """Raised when bookmark state cannot be loaded or saved."""


class S3WriteError(CDCToS3Error):
    """Raised when S3 IO fails."""

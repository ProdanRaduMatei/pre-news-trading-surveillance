from __future__ import annotations

import mimetypes
import os
from pathlib import Path


class PublishStorageError(RuntimeError):
    """Raised when publish storage upload configuration is invalid."""


def upload_directory_to_s3(
    *,
    source_dir: Path,
    bucket: str,
    prefix: str = "",
    region: str | None = None,
    endpoint_url: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
    session_token: str | None = None,
) -> list[str]:
    client = _build_s3_client(
        region=region,
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        session_token=session_token,
    )

    uploaded_keys: list[str] = []
    for file_path in sorted(path for path in source_dir.rglob("*") if path.is_file()):
        relative_key = file_path.relative_to(source_dir).as_posix()
        object_key = _join_prefix(prefix, relative_key)
        content_type, _ = mimetypes.guess_type(str(file_path))
        extra_args = {}
        if content_type:
            extra_args["ContentType"] = content_type
        client.upload_file(
            str(file_path),
            bucket,
            object_key,
            ExtraArgs=extra_args or None,
        )
        uploaded_keys.append(object_key)
    return uploaded_keys


def resolve_optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _build_s3_client(
    *,
    region: str | None,
    endpoint_url: str | None,
    access_key: str | None,
    secret_key: str | None,
    session_token: str | None,
):
    try:
        import boto3
    except ImportError as exc:
        raise PublishStorageError(
            "boto3 is required for S3 publication. Install dependencies with `pip install -e .[publish]`."
        ) from exc

    kwargs: dict[str, object] = {}
    if region:
        kwargs["region_name"] = region
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    if access_key:
        kwargs["aws_access_key_id"] = access_key
    if secret_key:
        kwargs["aws_secret_access_key"] = secret_key
    if session_token:
        kwargs["aws_session_token"] = session_token
    return boto3.client("s3", **kwargs)


def _join_prefix(prefix: str, key: str) -> str:
    cleaned_prefix = prefix.strip("/")
    if not cleaned_prefix:
        return key
    return f"{cleaned_prefix}/{key}"

import os


def is_running_on_radix_platform() -> bool:
    # Probe for the presence of a few well known RADIX environment variables, including the RADIX_APP environment variable,
    # which is expected to be set in all Radix environments. This is not a perfect method but should work for our purposes.
    return bool(os.getenv("RADIX_APP") and os.getenv("RADIX_ENVIRONMENT"))


def get_radix_component_name() -> str | None:
    return os.getenv("RADIX_COMPONENT")


def get_radix_environment_name() -> str | None:
    return os.getenv("RADIX_ENVIRONMENT")


def get_radix_short_commit_sha() -> str | None:
    full_git_commit_hash = os.getenv("RADIX_GIT_COMMIT_HASH")
    if full_git_commit_hash:
        return full_git_commit_hash[:7]
    return None

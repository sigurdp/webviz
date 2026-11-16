from cryptography.fernet import Fernet

# Module-level singleton
_fernet: Fernet | None = None  # pylint: disable=invalid-name


def initialize(fernet_key: str) -> None:
    """
    Initialize the module-level Fernet instance with the given key.

    Python one-liner to generate a new Fernet key:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """
    global _fernet  # pylint: disable=global-statement
    _fernet = Fernet(fernet_key)


def decrypt_data_to_str(data: str | bytes, ttl: int | None = None) -> str:
    if _fernet is None:
        raise RuntimeError("Fernet not initialized, call initialize() first")

    if isinstance(data, str):
        data = data.encode()

    decrypted_bytes = _fernet.decrypt(data, ttl)

    return decrypted_bytes.decode()

def decrypt_data_to_bytes(data: str | bytes, ttl: int | None = None) -> bytes:
    if _fernet is None:
        raise RuntimeError("Fernet not initialized, call initialize() first")

    if isinstance(data, str):
        data = data.encode()

    decrypted_bytes = _fernet.decrypt(data, ttl)

    return decrypted_bytes

def encrypt_data(data: bytes) -> bytes:
    if _fernet is None:
        raise RuntimeError("Fernet not initialized, call initialize() first")

    encrypted_bytes = _fernet.encrypt(data)

    return encrypted_bytes

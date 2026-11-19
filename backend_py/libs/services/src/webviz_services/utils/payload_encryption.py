from typing import TypeVar

from cryptography.fernet import Fernet
from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


class PayloadEncryption:
    """
    Helper class to encrypt/decrypt bytes or Pydantic models using Fernet

    Python one-liner to generate a new Fernet key:
        python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    """

    def __init__(self, fernet_key: bytes | str) -> None:
        self._fernet = Fernet(fernet_key)

    def encrypt(self, data: bytes) -> bytes:
        return self._fernet.encrypt(data)

    def decrypt(self, encrypted_data: bytes | str, ttl_s: int | None = None) -> bytes:
        """
        Decrypt the specified encrypted data, optionally enforcing a maximum age in seconds.
        If ttl is specified, the decrypted data must not be older than the given number of seconds.
        """
        return self._fernet.decrypt(encrypted_data, ttl=ttl_s)

    def decrypt_to_str(self, encrypted_data: bytes | str, ttl_s: int | None = None) -> str:
        decrypted_bytes: bytes = self.decrypt(encrypted_data, ttl_s=ttl_s)
        return decrypted_bytes.decode()

    def encrypt_pydantic_model(self, model: BaseModel) -> bytes:
        return self.encrypt(model.model_dump_json().encode())

    def decrypt_pydantic_model(
        self, encrypted_data: bytes | str, model_class: type[ModelT], ttl_s: int | None = None
    ) -> ModelT:
        decrypted_bytes: bytes = self.decrypt(encrypted_data, ttl_s=ttl_s)
        return model_class.model_validate_json(decrypted_bytes)

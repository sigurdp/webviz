from webviz_services.utils.payload_encryption import PayloadEncryption


class PayloadEncryptionSingleton:
    _instance: PayloadEncryption | None = None

    @classmethod
    def initialize(cls, fernet_key: bytes | str) -> None:
        if cls._instance is not None:
            raise RuntimeError("PayloadEncryptionSingleton is already initialized")
        cls._instance = PayloadEncryption(fernet_key)

    @classmethod
    def get_instance(cls) -> PayloadEncryption:
        if cls._instance is None:
            raise RuntimeError("PayloadEncryptionSingleton is not initialized")
        return cls._instance

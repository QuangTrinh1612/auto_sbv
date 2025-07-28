import base64
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import str, bytes
import logging


class EncryptionUtil:
    """
    Utility class for encrypting and decrypting sensitive data.
    Used primarily for password encryption in configuration files.
    """
    
    def __init__(self, password: str = None):
        self.logger = logging.getLogger(__name__)
        
        # Use provided password or get from environment
        if password is None:
            password = os.getenv('ETL_ENCRYPTION_KEY', 'default-etl-framework-key')
        
        self.fernet = self._create_fernet(password)
    
    def _create_fernet(self, password: str) -> Fernet:
        """Create Fernet encryption instance from password."""
        password_bytes = password.encode()
        salt = b'etl_framework_salt'  # In production, use a random salt
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password_bytes))
        return Fernet(key)
    
    def encrypt_password(self, password: str) -> str:
        """
        Encrypt a password.
        
        Args:
            password: Plain text password
            
        Returns:
            str: Encrypted password (base64 encoded)
        """
        try:
            encrypted_bytes = self.fernet.encrypt(password.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
        except Exception as e:
            self.logger.error(f"Failed to encrypt password: {str(e)}")
            raise
    
    def decrypt_password(self, encrypted_password: str) -> str:
        """
        Decrypt a password.
        
        Args:
            encrypted_password: Encrypted password (base64 encoded)
            
        Returns:
            str: Plain text password
        """
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_password.encode())
            decrypted_bytes = self.fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            self.logger.error(f"Failed to decrypt password: {str(e)}")
            raise


# Global encryption utility instance
_encryption_util = None


def get_encryption_util() -> EncryptionUtil:
    """Get the global encryption utility instance."""
    global _encryption_util
    
    if _encryption_util is None:
        _encryption_util = EncryptionUtil()
    
    return _encryption_util


def encrypt_password(password: str) -> str:
    """Convenience function to encrypt a password."""
    util = get_encryption_util()
    return util.encrypt_password(password)


def decrypt_password(encrypted_password: str) -> str:
    """Convenience function to decrypt a password."""
    util = get_encryption_util()
    return util.decrypt_password(encrypted_password)
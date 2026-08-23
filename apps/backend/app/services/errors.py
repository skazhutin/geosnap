from __future__ import annotations

from app.schemas import ApiStatus


class PublicAPIError(Exception):
    def __init__(self, status: ApiStatus, message: str, http_status: int) -> None:
        super().__init__(message)
        self.status = status
        self.public_message = message
        self.http_status = http_status


class InvalidImageError(PublicAPIError):
    def __init__(self, message: str = "The uploaded file is not a valid image.") -> None:
        super().__init__(ApiStatus.INVALID_IMAGE, message, 422)


class UnsupportedFormatError(PublicAPIError):
    def __init__(self, message: str = "The image format is not supported.") -> None:
        super().__init__(ApiStatus.UNSUPPORTED_FORMAT, message, 415)


class ImageTooLargeError(PublicAPIError):
    def __init__(self, message: str = "The uploaded image is too large.") -> None:
        super().__init__(ApiStatus.IMAGE_TOO_LARGE, message, 413)


class ModelNotReadyError(PublicAPIError):
    def __init__(self) -> None:
        super().__init__(ApiStatus.MODEL_NOT_READY, "The localization model is not ready.", 503)


class IndexNotReadyError(PublicAPIError):
    def __init__(self) -> None:
        super().__init__(ApiStatus.INDEX_NOT_READY, "The reference index is not ready.", 503)

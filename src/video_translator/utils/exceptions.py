"""exceptions"""


class TranslatorException(Exception):
    """exceptions"""

    def __init__(self, message: object) -> object:
        self.message = message
        super().__init__(message)

    def __repr__(self):
        """repr"""
        return f'{self.__class__.__name__}("detail"={self.message})'

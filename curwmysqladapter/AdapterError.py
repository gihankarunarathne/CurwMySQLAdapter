class AdapterError(Exception):
    pass


class InvalidDataAdapterError(AdapterError):
    def __init__(self, message):
        self.message = message


class DatabaseConstrainAdapterError(AdapterError):
    def __init__(self, message):
        self.message = message


class DatabaseAdapterError(AdapterError):
    def __init__(self, message):
        self.message = message

    def __init__(self, message, exception):
        self.message = message
        self.exception = exception

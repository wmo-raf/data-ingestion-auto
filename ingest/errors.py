class Error(Exception):

    def __init__(self, message):
        self.message = message


class ParameterMissing(Error):
    pass


class UnknownDataConvertOperation(Error):
    pass


class UnKnownGeomType(Error):
    pass

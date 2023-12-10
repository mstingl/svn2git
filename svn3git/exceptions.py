class ConverterError(Exception):
    pass


class TagExistsError(ConverterError):
    def __init__(self, tag, commit):
        self.tag = tag
        self.commit = commit


class MissingBranchError(ConverterError):
    pass


class MergeError(ConverterError):
    pass


class MissingAuthorError(ConverterError):
    pass


class SvnFetchError(ConverterError):
    pass


class SvnOptionsReadError(ConverterError):
    pass


class SvnExternalError(ConverterError):
    pass

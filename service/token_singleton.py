class TokenSingleton(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(TokenSingleton, cls).__new__(cls)
        return cls.instance

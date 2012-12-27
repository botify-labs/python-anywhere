all = 'RegisterDict'


class RegisterDict(dict):
    def register(self, key, value):
        self[key] = value

"""Work around an issue with defining class attribute documentation.

See http://stackoverflow.com/questions/9153473/sphinx-values-for-attributes-reported-as-none/39276413
"""  # noqa: E501 line too long


class ValueDoc(object):

    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text

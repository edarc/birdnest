class Tabulator(object):
    def __init__(self, *args):
        self._field_widths = list(args)

    def __call__(self, *args):
        return ''.join(self.pad(arg, width)
                       for arg, width
                       in zip(args, self._field_widths)).rstrip()

    def pad(self, s, width):
        s_trunc = str(s)[:width]
        return s_trunc + ' ' * (int(width) - len(s_trunc))

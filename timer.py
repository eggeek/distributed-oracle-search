import timeit


class Timer:
    """
    Timer class based on Phreshing.

    http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/
    """

    def __init__(self, interval=None):
        """
        Initialise a new Timer.
        """
        if interval is not None:
            self.interval = interval
        else:
            self.interval = 0

    def __enter__(self):
        self.start = timeit.default_timer()
        return self

    def __exit__(self, *args):
        self.end = timeit.default_timer()
        self.interval = self.end - self.start

    def __add__(self, other):
        """
        Not sure this is canonical in Python: return a *new* Timer when using
        __add__.
        """
        if isinstance(other, Timer):
            return Timer(self.interval + other.interval)
        else:
            return Timer(self.interval + other)

    def __radd__(self, other):
        """
        This may be even less canonical: return a new timer if right-added
        (think `+=`), otherwise add the interval's value (e.g., used when
        incrementing a time counter).
        """
        if isinstance(other, Timer):
            return self.__add__(other)
        else:
            return other + self.interval

    def __str__(self):
        """
        Format the interval a bit nicer.  The precision is supposed to be
        seconds -- cf. https://docs.python.org/3.5/library/time.html#time.clock
        """
        if self.interval < 10e-8:
            # We do not format as we do not know how precise the interval is --
            # although, let's be honest, I do not thing you can go lower than
            # that.
            return "{:f}".format(self.interval * 10e9)
        elif self.interval < 10e-6:
            mod = 10e6
            division = "us"  # An pactual utf-8 \mu looks too ugly in JSON
        elif self.interval < 10e-3:
            mod = 10e3
            division = "ms"
        else:
            mod = 1
            division = "s"

        return "{:.3f}{}".format(self.interval * mod, division)

    def __repr__(self):
        return "T[{}]".format(self.interval)

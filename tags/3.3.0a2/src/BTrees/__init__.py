try:
    import intSet
except:
    pass
else:
    del intSet

# Register interfaces
try:
    import Interface
except ImportError:
    pass # Don't register interfaces if no scarecrow
else:
    import Interfaces
    del Interfaces
    del Interface

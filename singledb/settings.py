SLAVE_MODE=False
MASTER_URL="https://iq.channelfactory.com/"

try:
    from .local_settings import *
except ImportError:
    pass

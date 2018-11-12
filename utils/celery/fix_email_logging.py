def fix_email_logging():
    from billiard.einfo import _Frame, Traceback

    class _FrameFixed(_Frame):
        def __init__(self, *args, **kwargs):
            super(_FrameFixed, self).__init__(*args, **kwargs)
            self.f_back = None

    Traceback.Frame = _FrameFixed

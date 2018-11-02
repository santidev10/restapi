from django.conf import settings


class BaseEmailReport:

    DEBUG_PREFIX = "DEBUG_"

    def __init__(self, host, debug, **kwargs):
        self.host = host
        self.debug = debug
        self.kwargs = kwargs

    def get_to(self, to_emails):
        return self.get_recipients(to_emails)

    def get_cc(self, cc):
        return self.get_recipients(cc)

    def get_recipients(self, emails):
        if self.debug:
            new_emails = []
            for e in emails:
                if isinstance(e, str):
                    new_emails.append(self.DEBUG_PREFIX + e)
                elif isinstance(e, tuple):
                    new_emails.append((e[0], self.DEBUG_PREFIX + e[1]))
            return new_emails
        else:
            return emails

    def get_bcc(self):
        bcc = []
        if self.debug:
            bcc = settings.ADMINS
        return bcc

    def send(self):
        raise NotImplementedError("Send emails here")

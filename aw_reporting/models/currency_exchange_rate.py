from django.db import models


class CurrencyExchangeRate(models.Model):
    """
    save historical currency exchange rates. one for each from/to pair, per date
    """
    date = models.DateField(db_index=True)
    from_code = models.CharField(max_length=3, db_index=True)
    to_code = models.CharField(max_length=3, db_index=True)
    rate = models.DecimalField(max_digits=30, decimal_places=15)

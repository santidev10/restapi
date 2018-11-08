def quart_views(row, n):
    per = getattr(row, "VideoQuartile%dRate" % n)
    impressions = int(row.Impressions)
    return float(per.rstrip("%")) / 100 * impressions

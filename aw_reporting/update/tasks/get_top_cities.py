import heapq
from collections import defaultdict


def get_top_cities(report):
    top_cities = []
    top_number = 10

    summary_cities_costs = defaultdict(int)
    report_by_campaign = defaultdict(list)
    for r in report:
        report_by_campaign[r.CampaignId].append(r)
        summary_cities_costs[r.CityCriteriaId] += int(r.Cost)

    # top for every campaign
    for camp_rep in report_by_campaign.values():
        top = heapq.nlargest(
            top_number, camp_rep,
            lambda i: int(i.Cost) if i.CityCriteriaId.isnumeric() else 0
        )
        for s in top:
            top_cities.append(s.CityCriteriaId)

    # global top
    global_top = heapq.nlargest(
        top_number,
        summary_cities_costs.items(),
        lambda i: i[1] if i[0].isnumeric() else 0
    )
    for item in global_top:
        top_cities.append(item[0])
    return set(int(i) for i in top_cities if i.isnumeric())

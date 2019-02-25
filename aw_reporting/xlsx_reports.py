from io import BytesIO

import xlsxwriter

from aw_reporting.reports.pacing_report import PacingReport


def pacing_report(report, opportunities):
    from aw_reporting.models import Flight
    from aw_reporting.models import OpPlacement
    from aw_reporting.models import Opportunity
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet()

    # formats
    global_f = {'font_size': 10}
    fm = {
        'money': {'num_format': '$#,##0.00'},
        'precision_money': {'num_format': '$#,##0.000'},
        'pacing': {'bg_color': '#ffff00',
                   'num_format': '0.00%'},
        'header': {'bold': True,
                   'text_wrap': True,
                   'align': 'center'},
        'sub_header': {'text_wrap': True,
                       'align': 'center'},
        'percent': {'num_format': '0.00%'},
        'number': {'num_format': '#,##0'},
        'date': {'num_format': 'yyyy-mm-dd'},
        'default': {},
    }
    for v in fm.values():
        v.update(global_f)
    fm = {n: workbook.add_format(v) for n, v in fm.items()}

    row = 0
    col = 0

    worksheet.merge_range(
        'A{row}:D{row}'.format(row=row + 1), 'Name', fm['header']
    )
    worksheet.merge_range(
        'E{row}:F{row}'.format(row=row + 1), 'KPIs', fm['header']
    )
    worksheet.merge_range(
        'G{row}:I{row}'.format(row=row + 1), 'Dates', fm['header']
    )
    worksheet.merge_range(
        'J{row}:N{row}'.format(row=row + 1), 'Goals', fm['header']
    )
    worksheet.merge_range(
        'O{row}:S{row}'.format(row=row + 1), 'Delivered', fm['header']
    )
    row += 1

    header = (
        'Opportunity', 'Placement', 'Flight', 'Campaign', 'Pacing',
        'Margin', 'IO', 'Start', 'End', 'Budget', 'Views', 'CPV',
        'Impressions', 'CPM', 'Cost', 'Views', 'CPV', 'Impressions',
        'CPM', 'AdOps', 'AM', 'Sales', 'Category', 'Territory',
    )

    for h in header:
        worksheet.write(row, col, h, fm['sub_header'])
        col += 1
    row += 1

    fields = (
        'pacing', 'margin', 'io_start', 'start', 'end', 'plan_cost',
        'plan_video_views', 'plan_cpv', 'plan_impressions', 'plan_cpm',
        'cost', 'video_views', 'cpv', 'impressions', 'cpm'
    )
    formats = dict(
        pacing=fm['percent'],
        margin=fm['percent'],
        io_start=fm['date'],
        start=fm['date'],
        end=fm['date'],
        plan_cost=fm['money'],
        plan_cpv=fm['precision_money'],
        plan_cpm=fm['precision_money'],
        cost=fm['money'],
        cpv=fm['precision_money'],
        cpm=fm['precision_money'],
        plan_video_views=fm['number'],
        video_views=fm['number'],
        plan_impressions=fm['number'],
        impressions=fm['number'],
    )

    for opportunity in opportunities:
        # opportunity
        col = 0
        for f in ('name', None, None, None) + fields + ("ad_ops", "am", "sales", "category", "region"):
            v = opportunity.get(f)
            if type(v) is dict:
                v = v['name']
            elif v is None and f in ('pacing', 'margin'):
                v = " "
            if v is not None:
                worksheet.write(row, col, v, formats.get(f, fm['default']))

            col += 1
        row += 1

        # placements
        opportunity_object = Opportunity.objects.get(id=opportunity["id"])
        for placement in report.get_placements(opportunity_object):
            col = 0
            for f in (None, 'name', None, None) + fields:
                v = placement.get(f)
                if v is None and f in ('pacing', 'margin'):
                    v = " "
                if v is not None:
                    worksheet.write(
                        row, col, v, formats.get(f, fm['default'])
                    )
                col += 1

            worksheet.set_row(
                row, options=dict(level=1, collapsed=True, hidden=True)
            )
            row += 1

            # flights
            placement_obj = OpPlacement.objects.get(id=placement["id"])
            for flight in report.get_flights(placement_obj):
                col = 0
                for f in (None, None, 'name', None) + fields:
                    v = flight.get(f)
                    if v is None and f in ('pacing', 'margin'):
                        v = " "
                    if v is not None:
                        worksheet.write(
                            row, col, v, formats.get(f, fm['default'])
                        )
                    col += 1

                worksheet.set_row(
                    row, options=dict(level=2, collapsed=True, hidden=True)
                )
                row += 1

                # campaigns
                flight_obj = Flight.objects.get(id=flight["id"])
                for campaign in report.get_campaigns(flight_obj):
                    col = 0
                    for f in (None, None, None, 'name') + fields:
                        v = campaign.get(f)
                        if v is None and f in ('pacing', 'margin'):
                            v = " "
                        if v is not None:
                            worksheet.write(
                                row, col, v, formats.get(f, fm['default'])
                            )
                        col += 1

                    worksheet.set_row(
                        row,
                        options=dict(level=3, collapsed=True, hidden=True)
                    )
                    row += 1

    ##########

    # set widths
    worksheet.set_column(0, 2, width=15)
    worksheet.set_column(3, 3, width=45)
    worksheet.set_column(4, 18, width=10)
    worksheet.set_column(19, 22, width=15)

    ########
    red = workbook.add_format({'bg_color': '#B20000'})
    green = workbook.add_format({'bg_color': '#004C00'})
    yellow = workbook.add_format({'bg_color': '#ffa500'})

    borders = PacingReport.borders

    # pacing=((.8, .9), (1.1, 1.2)),
    low_pacing_border, high_pacing_border = borders['pacing']
    worksheet.conditional_format('E3:E%d' % row, {'type': 'cell',
                                                  'criteria': '<=',
                                                  'value': low_pacing_border[0],
                                                  'format': red})
    worksheet.conditional_format('E3:E%d' % row, {'type': 'cell',
                                                  'criteria': '>=',
                                                  'value': high_pacing_border[1],
                                                  'format': red})

    worksheet.conditional_format('E3:E%d' % row, {'type': 'cell',
                                                  'criteria': '<',
                                                  'value': low_pacing_border[1],
                                                  'format': yellow})
    worksheet.conditional_format('E3:E%d' % row, {'type': 'cell',
                                                  'criteria': '>',
                                                  'value': high_pacing_border[0],
                                                  'format': yellow})

    worksheet.conditional_format('E3:E%d' % row, {'type': 'cell',
                                                  'criteria': 'between',
                                                  'minimum': low_pacing_border[1],
                                                  'maximum': high_pacing_border[0],
                                                  'format': green})

    margin_border = borders['margin']
    worksheet.conditional_format('F3:F%d' % row, {'type': 'cell',
                                                  'criteria': '<',
                                                  'value': margin_border[1],
                                                  'format': red})

    worksheet.conditional_format('F3:F%d' % row, {'type': 'cell',
                                                  'criteria': '>=',
                                                  'value': margin_border[0],
                                                  'format': green})
    worksheet.conditional_format('F3:F%d' % row, {'type': 'cell',
                                                  'criteria': 'between',
                                                  'minimum': margin_border[0],
                                                  'maximum': margin_border[1],
                                                  'format': yellow})
    workbook.close()
    return output.getvalue()

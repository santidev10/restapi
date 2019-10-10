from typing import List

from utils.lang import merge_dicts

__all__ = [
    "TargetSheetTableStyles",
]


class Styles:
    STYLE_DEFINITION = {}

    def __init__(self, workbook):
        self.workbook = workbook
        self._style_cache = {}

    def get_style(self, cursor, value, is_header, header=None):
        style_names = self._resolve_style_names(cursor, value, is_header, header)
        style_key = ":".join(style_names)
        if style_key not in self._style_cache:
            style_definitions = [
                self.STYLE_DEFINITION.get(style_name, {})
                for style_name in style_names
            ]
            style_definition = merge_dicts(*style_definitions)
            style = self.workbook.add_format(style_definition)
            self._style_cache[style_key] = style
        return self._style_cache[style_key]

    def _resolve_style_names(self, cursor, value, is_header, header) -> List[str]:
        raise NotImplementedError


class SheetTableStyles(Styles):
    class _Styles:
        GOOD = "GOOD"
        WARNING = "WARNING"
        CRITICAL = "CRITICAL"
        PERCENTAGE_SHORT = "PERCENTAGE_SHORT"
        PERCENTAGE_LONG = "PERCENTAGE_LONG"

    STYLE_DEFINITION = {
        _Styles.GOOD: {
            "bg_color": "#00B25B",
        },
        _Styles.WARNING: {
            "bg_color": "#FFFE50",
        },
        _Styles.CRITICAL: {
            "bg_color": "#FF0013",
        },
        _Styles.PERCENTAGE_SHORT: {
            "num_format": "0%",
        },
        _Styles.PERCENTAGE_LONG: {
            "num_format": "0.00%",
        },
    }

    def _resolve_style_names(self, cursor, value, is_header, header) -> List[str]:
        get_style_method_name = f"_get_{header}_column_style_classes"
        get_style_method = getattr(self, get_style_method_name, None)
        if get_style_method is None:
            return []
        return [style for style in get_style_method(value, is_header) if style]

    def _get_margin_column_style_classes(self, value, is_header):
        if is_header:
            return []
        bounds = (
            (.3, self._Styles.CRITICAL),
            (.4, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        color_style = calculate_range_stats(bounds, value)
        return [color_style, self._Styles.PERCENTAGE_SHORT]

    def _get_cost_delivery_percentage_column_style_classes(self, value, is_header):
        if is_header:
            return []
        bounds = (
            (.5, self._Styles.CRITICAL),
            (.7, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        color_style = calculate_range_stats(bounds, value)
        return [color_style, self._Styles.PERCENTAGE_LONG]

    def _get_delivery_percentage_column_style_classes(self, value, is_header):
        if is_header:
            return []
        bounds = (
            (.5, self._Styles.CRITICAL),
            (.7, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        color_style = calculate_range_stats(bounds, value)
        return [color_style, self._Styles.PERCENTAGE_LONG]

    def _get_video_played_to_100_column_style_classes(self, value, is_header):
        if is_header:
            return []
        return [self._Styles.PERCENTAGE_SHORT]

    def _get_view_rate_column_style_classes(self, value, is_header):
        if is_header:
            return []
        return [self._Styles.PERCENTAGE_LONG]

    def _get_ctr_column_style_classes(self, value, is_header):
        if is_header:
            return []
        return [self._Styles.PERCENTAGE_LONG]


def calculate_range_stats(bounds, value):
    if value is None:
        return None
    for bound, style in bounds:
        if value <= bound:
            return style
    return None

class Styles:
    STYLE_DEFINITION = {}

    def __init__(self, workbook):
        self.workbook = workbook
        self._style_cache = {}

    def get_style(self, cursor, value, is_header, header=None):
        style_name = self._resolve_style_name(cursor, value, is_header, header)
        if style_name not in self._style_cache:
            style_definition = self.STYLE_DEFINITION.get(style_name, {})
            style = self.workbook.add_format(style_definition)
            self._style_cache[style_name] = style
        return self._style_cache[style_name]

    def _resolve_style_name(self, cursor, value, is_header, header) -> str:
        raise NotImplementedError


class TargetSheetTableStyles(Styles):
    class _Styles:
        GOOD = "GOOD"
        WARNING = "WARNING"
        CRITICAL = "CRITICAL"

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
    }

    def _resolve_style_name(self, cursor, value, is_header, header) -> str:
        get_style_method_name = f"_get_{header}_column_style"
        get_style_method = getattr(self, get_style_method_name, None)
        if get_style_method is None:
            return None
        return get_style_method(value, is_header)

    def _get_margin_column_style(self, value, is_header):
        if is_header:
            return None
        bounds = (
            (.3, self._Styles.CRITICAL),
            (.4, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        return self._calculate_range_stats(bounds, value)

    def _get_cost_delivery_percentage_column_style(self, value, is_header):
        if is_header:
            return None
        bounds = (
            (.5, self._Styles.CRITICAL),
            (.7, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        return self._calculate_range_stats(bounds, value)

    def _get_delivery_percentage_column_style(self, value, is_header):
        if is_header:
            return None
        bounds = (
            (.5, self._Styles.CRITICAL),
            (.7, self._Styles.WARNING),
            (1, self._Styles.GOOD),
        )
        return self._calculate_range_stats(bounds, value)

    def _calculate_range_stats(self, bounds, value):
        if value is None:
            return None
        for bound, style in bounds:
            if value < bound:
                return style
        return None

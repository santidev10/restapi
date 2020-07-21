import tokenize

from pylint.checkers import BaseTokenChecker
from pylint.interfaces import IAstroidChecker
from pylint.interfaces import ITokenChecker


class TrailingNewLinesChecker(BaseTokenChecker):
    __implements__ = (ITokenChecker, IAstroidChecker)

    name = "trailing-new-lines"
    priority = -1
    msgs = {
        "C1001": (
            "Trailing new lines (%s <= n <= %s)",
            "trailing-new-lines",
            "Importing multiple items per line."
        ),
    }
    options = (
        (
            "min-trailing-new-line",
            {
                "default": 1,
                "type": "int",
                "metavar": "<int>",
                "help": "Minimum number of empty lines in the end of file."
            }
        ),
        (
            "max-trailing-new-line",
            {
                "default": 2,
                "type": "int",
                "metavar": "<int>",
                "help": "Maximum number of empty lines in the end of file."
            }
        ),
    )

    def _calculate_new_lines(self, tokens):
        count = 0
        for token in reversed(tokens):
            if token.type not in (tokenize.NL, tokenize.NEWLINE, tokenize.DEDENT, tokenize.ENDMARKER):
                break
            count += len(token.string)

        return count

    def process_tokens(self, tokens):
        # pylint: disable=no-member
        empty_lines_count = self._calculate_new_lines(tokens)
        is_empty_file = tokens[-1].start[0] == 1
        is_allowed = self.config.min_trailing_new_line <= empty_lines_count <= self.config.max_trailing_new_line
        if (is_empty_file and empty_lines_count > 0) \
                or (not is_empty_file and not is_allowed):
            self.add_message(
                "trailing-new-lines",
                line=tokens[-1].start[0],
                args=(self.config.min_trailing_new_line, self.config.max_trailing_new_line)
            )
        # pylint: enable=no-member


def register(linter):
    linter.register_checker(TrailingNewLinesChecker(linter))

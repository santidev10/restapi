from pylint.checkers import BaseChecker
from pylint.interfaces import IAstroidChecker


class MultiImportPerLineChecker(BaseChecker):
    __implements__ = IAstroidChecker

    name = "multi-imports"
    priority = -1
    msgs = {
        "C0001": (
            "Importing multiple items per line.",
            "multi-import",
            "Importing multiple items per line."
        ),
    }

    def _assert_node(self, node):
        if len(node.names) > 1:
            self.add_message("multi-import", node=node)

    def visit_importfrom(self, node):
        self._assert_node(node)

    def visit_import(self, node):
        self._assert_node(node)


def register(linter):
    linter.register_checker(MultiImportPerLineChecker(linter))

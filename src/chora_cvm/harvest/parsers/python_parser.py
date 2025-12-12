"""
Python parser: Extract functions, classes, and docstrings using ast.

Chunk types:
- python-module-doc: Module-level docstring
- python-function: Function definition with signature and docstring
- python-class: Class definition with docstring and method signatures
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class Chunk:
    """A chunk of content extracted from a document."""

    title: str
    content: str
    line_start: int
    line_end: int
    chunk_type: str = "python-function"
    heading_level: Optional[int] = None
    metadata: dict = field(default_factory=dict)


class PythonParser:
    """Parse Python files using ast to extract functions, classes, and docstrings."""

    def parse(self, content: str, filepath: Path) -> list[Chunk]:
        """Parse Python content into chunks."""
        chunks = []

        try:
            tree = ast.parse(content)
        except SyntaxError:
            # If parsing fails, return empty (or could return raw content)
            return []

        lines = content.split("\n")

        # Extract module docstring
        module_doc = ast.get_docstring(tree)
        if module_doc:
            chunks.append(
                Chunk(
                    title=f"Module: {filepath.stem}",
                    content=module_doc,
                    line_start=1,
                    line_end=self._count_docstring_lines(module_doc),
                    chunk_type="python-module-doc",
                    metadata={"module": filepath.stem},
                )
            )

        # Walk the AST
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) or isinstance(node, ast.AsyncFunctionDef):
                chunk = self._parse_function(node, lines, filepath)
                if chunk:
                    chunks.append(chunk)

            elif isinstance(node, ast.ClassDef):
                chunk = self._parse_class(node, lines, filepath)
                if chunk:
                    chunks.append(chunk)

        return chunks

    def _parse_function(
        self, node: ast.FunctionDef | ast.AsyncFunctionDef, lines: list[str], filepath: Path
    ) -> Optional[Chunk]:
        """Parse a function definition."""
        docstring = ast.get_docstring(node)

        # Skip private functions without docstrings
        if node.name.startswith("_") and not docstring:
            return None

        # Build signature
        signature = self._get_function_signature(node)
        is_async = isinstance(node, ast.AsyncFunctionDef)

        # Build content
        content_parts = []
        if is_async:
            content_parts.append(f"async def {signature}")
        else:
            content_parts.append(f"def {signature}")

        if docstring:
            content_parts.append("")
            content_parts.append(docstring)

        # Get decorators
        decorators = [self._get_decorator_name(d) for d in node.decorator_list]
        if decorators:
            content_parts.insert(0, f"Decorators: {', '.join(decorators)}")

        return Chunk(
            title=f"Function: {node.name}",
            content="\n".join(content_parts),
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            chunk_type="python-function",
            metadata={
                "name": node.name,
                "signature": signature,
                "is_async": is_async,
                "decorators": decorators,
                "has_docstring": bool(docstring),
            },
        )

    def _parse_class(
        self, node: ast.ClassDef, lines: list[str], filepath: Path
    ) -> Optional[Chunk]:
        """Parse a class definition."""
        docstring = ast.get_docstring(node)

        # Skip private classes without docstrings
        if node.name.startswith("_") and not docstring:
            return None

        # Get base classes
        bases = [self._get_name(base) for base in node.bases]

        # Build content
        content_parts = []
        if bases:
            content_parts.append(f"class {node.name}({', '.join(bases)})")
        else:
            content_parts.append(f"class {node.name}")

        if docstring:
            content_parts.append("")
            content_parts.append(docstring)

        # Get method signatures
        methods = []
        for item in node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                sig = self._get_function_signature(item)
                prefix = "async " if isinstance(item, ast.AsyncFunctionDef) else ""
                methods.append(f"{prefix}def {sig}")

        if methods:
            content_parts.append("")
            content_parts.append("Methods:")
            for method in methods[:20]:  # Limit to first 20 methods
                content_parts.append(f"  {method}")

        # Get decorators
        decorators = [self._get_decorator_name(d) for d in node.decorator_list]
        if decorators:
            content_parts.insert(0, f"Decorators: {', '.join(decorators)}")

        return Chunk(
            title=f"Class: {node.name}",
            content="\n".join(content_parts),
            line_start=node.lineno,
            line_end=node.end_lineno or node.lineno,
            chunk_type="python-class",
            metadata={
                "name": node.name,
                "bases": bases,
                "decorators": decorators,
                "method_count": len(methods),
                "has_docstring": bool(docstring),
            },
        )

    def _get_function_signature(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
        """Get function signature as string."""
        args = []

        # Positional-only args (before /)
        for arg in node.args.posonlyargs:
            args.append(self._format_arg(arg))

        if node.args.posonlyargs:
            args.append("/")

        # Regular args
        num_defaults = len(node.args.defaults)
        num_args = len(node.args.args)

        for i, arg in enumerate(node.args.args):
            formatted = self._format_arg(arg)
            # Add default value placeholder if applicable
            default_idx = i - (num_args - num_defaults)
            if default_idx >= 0:
                formatted += "=..."
            args.append(formatted)

        # *args
        if node.args.vararg:
            args.append(f"*{node.args.vararg.arg}")
        elif node.args.kwonlyargs:
            args.append("*")

        # Keyword-only args
        for i, arg in enumerate(node.args.kwonlyargs):
            formatted = self._format_arg(arg)
            if i < len(node.args.kw_defaults) and node.args.kw_defaults[i] is not None:
                formatted += "=..."
            args.append(formatted)

        # **kwargs
        if node.args.kwarg:
            args.append(f"**{node.args.kwarg.arg}")

        # Return type
        returns = ""
        if node.returns:
            returns = f" -> {self._get_name(node.returns)}"

        return f"{node.name}({', '.join(args)}){returns}"

    def _format_arg(self, arg: ast.arg) -> str:
        """Format a function argument."""
        if arg.annotation:
            return f"{arg.arg}: {self._get_name(arg.annotation)}"
        return arg.arg

    def _get_name(self, node: ast.expr) -> str:
        """Get name from an AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Subscript):
            return f"{self._get_name(node.value)}[{self._get_name(node.slice)}]"
        elif isinstance(node, ast.Constant):
            return repr(node.value)
        elif isinstance(node, ast.Tuple):
            return f"({', '.join(self._get_name(e) for e in node.elts)})"
        elif isinstance(node, ast.List):
            return f"[{', '.join(self._get_name(e) for e in node.elts)}]"
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            # Union type hint (X | Y)
            return f"{self._get_name(node.left)} | {self._get_name(node.right)}"
        else:
            return "..."

    def _get_decorator_name(self, node: ast.expr) -> str:
        """Get decorator name from AST node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Call):
            return f"{self._get_decorator_name(node.func)}(...)"
        else:
            return "..."

    def _count_docstring_lines(self, docstring: str) -> int:
        """Count lines in a docstring."""
        return len(docstring.split("\n")) + 2  # +2 for quotes

    def extract_title(self, content: str, filename: str) -> str:
        """Extract document title from module docstring or filename."""
        try:
            tree = ast.parse(content)
            docstring = ast.get_docstring(tree)
            if docstring:
                # Use first line of docstring
                first_line = docstring.split("\n")[0].strip()
                if first_line:
                    return first_line[:100]  # Limit length
        except SyntaxError:
            pass
        return filename

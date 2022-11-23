import enum
import typing


NIL = 'nil'


class Dialects(str, enum.Enum):
    MYSQL = 'mysql'
    POSTGRES = 'postgres'
    SQLSERVER = 'sqlserver'


class IncorrectFieldFormat(Exception):
    """Raised when field is not in correct format.

    Correct format is: ['field', <field_id>]
    """


class FieldDoesNotExist(Exception):
    """Raised when field with ID does not exist."""


class OperatorDoesNotExist(Exception):
    """Raised when operator does not exist."""


def _is_correct_field(arg: list):
    return len(arg) == 2 and arg[0] == 'field' and isinstance(arg[1], int)


def _operator_to_clause(operator: str) -> str:
    if operator == '=':
        return 'IN'
    elif operator == '!=':
        return 'NOT IN'
    elif operator == 'is-empty':
        return 'IS NULL'
    elif operator == 'not-empty':
        return 'IS NOT NULL'

    raise OperatorDoesNotExist(f'Operator {operator} does not exist')


def _surround_clause(clause: str) -> str:
    return f'({clause})'


def _surround_identifier(name: str, dialect: str):
    if dialect == Dialects.MYSQL:
        return f'`{name}`'
    else:
        return f'"{name}"'


def _extract_field(arg, fields: dict[int, str], dialect: str) -> str:
    """Return argument as literal or field in correct format."""

    # arg is reference to Field
    if isinstance(arg, list):
        if not _is_correct_field(arg):
            raise IncorrectFieldFormat(f'Field data: {arg}')

        field_id = arg[1]

        if field_id not in fields:
            raise FieldDoesNotExist(f'Field with ID {field_id} does not exist')

        return _surround_identifier(fields[field_id], dialect)

    # arg is literal
    else:
        result = str(arg)
        if arg != NIL and isinstance(arg, str):
            result = f'\'{arg}\''

        return result


def _build_comparison_clause(
    operator: str,
    left_arg,
    right_arg,
    fields: dict[int, str],
    dialect: str,
) -> str:
    """Build binary clause.

    :param operator: can be one of =, !=, <, >
    """

    left = _extract_field(left_arg, fields, dialect)
    right = _extract_field(right_arg, fields, dialect)

    if NIL not in (left, right):
        return f'{left} {operator} {right}'

    # If either left arg or right arg is nil, then build null clause
    not_nil_arg = left_arg if left != NIL else right_arg
    new_operator = 'is-empty' if operator == '=' else 'not-empty'
    return _build_null_clause(new_operator, not_nil_arg, fields, dialect)


def _build_in_clause(operator: str, args: list, fields: dict[int, str], dialect: str) -> str:
    """Build IN or NOT in clause."""

    head_field = _extract_field(args[0], fields, dialect)
    tail_fields = [
        _extract_field(arg, fields, dialect)
        for arg in args[1:]
    ]
    args_clause = _surround_clause(', '.join(tail_fields))
    in_clause = _operator_to_clause(operator)

    return f'{head_field} {in_clause} {args_clause}'


def _build_null_clause(operator: str, arg, fields: dict[int, str], dialect: str) -> str:
    """Build IS NULL or IS NOT NULL clause."""

    field = _extract_field(arg, fields, dialect)
    null_clause = _operator_to_clause(operator)

    return f'{field} {null_clause}'


def _build_where_clause(dialect: str, fields: dict[int, str], query: typing.Optional[list]) -> str:
    if query is None:
        return ''

    operator = query[0]
    operands = query[1:]

    if operator in ('and', 'or'):
        clause_parts = []
        operator = f' {operator.upper()} '

        # If there is only one operand, ignore operator
        if len(operands) == 1:
            return _surround_clause(_build_where_clause(dialect, fields, operands[1]))

        for clause in operands:
            clause_parts.append(_build_where_clause(dialect, fields, clause))

        result_clause = operator.join(clause_parts)
    elif operator == 'not':
        result_clause = 'NOT ' + _build_where_clause(dialect, fields, operands[0])
    elif operator in ('<', '>'):
        result_clause = _build_comparison_clause(operator, operands[0], operands[1], fields, dialect)
    elif operator in ('=', '!='):
        if len(operands) == 2:
            # If there are two operands, then use equality clause
            result_clause = _build_comparison_clause(operator, operands[0], operands[1], fields, dialect)
        else:
            # Otherwise, use IN clause
            result_clause = _build_in_clause(operator, operands, fields, dialect)
    elif operator in ('is-empty', 'not-empty'):
        result_clause = _build_null_clause(operator, operands[0], fields, dialect)
    else:
        raise OperatorDoesNotExist(operator)

    return _surround_clause(result_clause)


def _build_limit_clause(dialect: str, limit: typing.Optional[int]) -> str:
    """Build limit clause according to dialect."""

    if limit is None:
        return ''

    if dialect == Dialects.SQLSERVER:
        return f'TOP({limit})'
    else:
        return f'LIMIT {limit}'


def generate_sql(dialect: str, fields: dict[int, str], query: dict) -> str:
    where_clause = _build_where_clause(dialect, fields, query.get('where'))
    limit_clause = _build_limit_clause(dialect, query.get('limit'))

    if where_clause != '':
        where_clause = 'WHERE ' + where_clause

    if dialect == Dialects.SQLSERVER:
        query_parts = ['SELECT', limit_clause, '* FROM data', where_clause]
    else:
        query_parts = ['SELECT * FROM data', where_clause, limit_clause]

    # Remove extra spaces in query
    query = ' '.join(part for part in query_parts if part.strip()) + ';'
    return query

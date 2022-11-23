import unittest

from . import generator


class TestGenerateSQL(unittest.TestCase):
    def setUp(self) -> None:
        self.fields = {
            1: 'id',
            2: 'name',
            3: 'date_joined',
            4: 'age',
        }

    def test_generate_sql_with_empty_fields_and_query(self):
        for dialect in ('mysql', 'postgres', 'sqlserver'):
            self.assertEqual(
                generator.generate_sql(dialect, {}, {}),
                'SELECT * FROM data;',
                f'dialect is {dialect}'
            )

    def test_generate_sql_ensure_string_literal_quoted(self):
        query = {
            'where': ['=', ['field', 2], 'cam'],
        }
        expected_query = "SELECT * FROM data WHERE (\"name\" = 'cam');"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_when_comparing_field_with_nil(self):
        query = {'where': ['=', ['field', 3], 'nil']}
        expected_query = "SELECT * FROM data WHERE (\"date_joined\" IS NULL);"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_when_comparing_field_with_integer(self):
        query = {'where': [">", ["field", 4], 35]}
        expected_query = "SELECT * FROM data WHERE (\"age\" > 35);"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_when_comparing_multiple_comparisons_and_conjunction(self):
        query = {"where": ["and", ["<", ["field", 1], 5], ["=", ["field", 2], "joe"]]}
        expected_query = "SELECT * FROM data WHERE ((\"id\" < 5) AND (\"name\" = 'joe'));"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_when_comparing_multiple_comparisons_and_disjunction(self):
        query = {"where": ["or", ["!=", ["field", 3], "2015-11-01"], ["=", ["field", 1], 456]]}
        expected_query = "SELECT * FROM data WHERE ((\"date_joined\" != '2015-11-01') OR (\"id\" = 456));"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_nested_disjunction(self):
        query = {
            "where":
                [
                    "and",
                    ["!=", ["field", 3], 'nil'],
                    ["or", [">", ["field", 4], 25], ["=", ["field", 2], "Jerry"]],
                ],
        }
        expected_query = (
            "SELECT * FROM data WHERE ((\"date_joined\" IS NOT NULL) "
            "AND ((\"age\" > 25) OR (\"name\" = 'Jerry')));"
        )
        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_in_operator(self):
        query = {"where": ["=", ["field", 4], 25, 26, 27]}
        expected_query = "SELECT * FROM data WHERE (\"age\" IN (25, 26, 27));"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_limit_for_mysql(self):
        query = {"where": ["=", ["field", 2], "cam"], "limit": 10}
        expected_query = "SELECT * FROM data WHERE (`name` = 'cam') LIMIT 10;"

        self.assertEqual(
            generator.generate_sql('mysql', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_limit_for_postgres(self):
        query = {"limit": 20}
        expected_query = "SELECT * FROM data LIMIT 20;"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_limit_for_sqlserver(self):
        query = {"limit": 10}
        expected_query = "SELECT TOP(10) * FROM data;"

        self.assertEqual(
            generator.generate_sql('sqlserver', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_is_empty_operator(self):
        query = {"where": ["is-empty", ["field", 4]]}
        expected_query = "SELECT * FROM data WHERE (\"age\" IS NULL);"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_not_empty_operator(self):
        query = {"where": ["not-empty", ["field", 4]]}
        expected_query = "SELECT * FROM data WHERE (\"age\" IS NOT NULL);"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_not_operator(self):
        query = {"where": ["not", ["=", ["field", 4], 25]]}
        expected_query = "SELECT * FROM data WHERE (NOT (\"age\" = 25));"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

    def test_generate_sql_with_nested_not_operator(self):
        query = {"where": ["not", ["and", ["not", ["<", ["field", 1], 5]], ["=", ["field", 2], "joe"]]]}
        expected_query = "SELECT * FROM data WHERE (NOT ((NOT (\"id\" < 5)) AND (\"name\" = 'joe')));"

        self.assertEqual(
            generator.generate_sql('postgres', self.fields, query),
            expected_query,
        )

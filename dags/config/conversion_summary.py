{
          'table': 'conversion_summary',
          'schema': 'hankyoul0919',
          'main_sql': """SELECT customer_id, SUM(clicked) AS clicked, SUM(purchased) AS purchased, SUM(paidamount) AS paidamount \
FROM hankyoul0919.customer_interactions \
GROUP BY 1 \
ORDER BY 1""",
          'input_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM hankyoul0919.customer_interactions',
              'count': 100000
            },
          ],
          'output_check':
          [
            {
              'sql': 'SELECT COUNT(1) FROM {schema}.temp_{table}',
              'count': 100000
            }
          ],
}

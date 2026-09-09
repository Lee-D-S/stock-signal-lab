# Point-in-time universe input

The online collector expects a point-in-time security master containing at least:

`ticker`, `market`, `security_type`, `listed_from`, `listed_to`, `suspended`, `price`, `avg_value_20d`, `history_days`.

Only KOSPI/KOSDAQ common stocks survive the default filters. Every rejected row is retained with a reason so the universe can be audited without survivorship assumptions.

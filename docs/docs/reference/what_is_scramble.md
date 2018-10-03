# What is a Scramble?

For VerdictDB's interactive query processing, a special table called a *scramble* must be created for an original table. The queries including scrambles are automatically rewritten by VerdictDB in a way to enable interactive querying. The queries including the original table(s) are also first rewritten to use its/their corresponding scrambles; then, VerdictDB applies to the same mechanism to enable interactive querying for those queries. If no scrambles have been created for a table, no query rewritting is performed.

Every time a scramble is created, VerdictDB stores the information in its own metadata schema (`verdictdbmetadata` by default). The relationship between the original tables and scrambles are recognized using this metadata.
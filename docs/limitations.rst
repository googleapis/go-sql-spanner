Limitations from Spanner perspective
------------------------------------

Below is a List of Limitations for features that are supported in Spanner but
are not supported in the Go Spanner driver.

Session Labeling
~~~~~~~~~~~~~~~~
Session Labeling is not a concept that is understood by Go driver it does not
support it inherently.

Request Priority
~~~~~~~~~~~~~~~~
Request Priority for database calls is not a concept that is understood by Go driver so
it does not support it inherently. The workaround is to unwrap the Spanner specific SpannerConn interface
and use request priority as part of the db calls.

Tagging
~~~~~~~
Tagging for database calls is not a concept that is understood by Go driver so it does not
support it inherently. The workaround is to unwrap the Spanner specific SpannerConn interface
and use the tagging feature via that.

Spangres
~~~~~~~
Spangres support needs setting `dialect` options when creating the Spanner client.
This will be released in future.


Backups / Cross region Backups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Backups for databases are not managed by Go driver so it does not support it inherently.
In general, none of the Spanner ORMs / drivers are expected to support backup management.
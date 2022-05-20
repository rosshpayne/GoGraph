# GoGraph for Google's Spanner Database

Take the NoSQL code base of DynamoGraph (written specifically for AWS's Dynamodb) and port it to Spanner, Google's NewSQL database. 

As a result DynamoGraph's single table NoSQL design becomes a five table relational design, incorporating extensive use of Spanner's array datatype and interleaved table features to minimise the impedence between NoSQL and NewSQL physical design and hence code changes.

GoGraph introduces the first development of an object-database-mapping layer (Tx package) designed to mininise the amount of boilerplate database code required to be written when accessing Spanner. The Tx package is significantly enhanced in later developments (in GoGraph3) to incorporate Dynamodb and relational databases like MySQL and handle both data manipulation and query access methods with zero boilerplat code requirement in a lot of cases.



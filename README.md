moz-graphs
==========

This is a big mess of code for collecting and analysing data about interactions between Mozilla developers and the bugs they work on. Messy data + graph theory + relational stuff. 

I use igraph for the graph theory stuff and sqlalchemy + sqlite + alembic for my db. 

This is really not intended for anyone else's use, and it doesn't include the raw data that you would need to populate the db (though it does include some scraping code to collect some of it). But if you do find anything here useful for whatever reason, please help yourself to it.

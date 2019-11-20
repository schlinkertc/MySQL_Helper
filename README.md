# MySQL_Helper
A module that allows a user to easily transfer Pandas DataFrames to and from a MY_SQL database

I created this module to allow for easier interaction between Pandas DataFrames and MY_SQL databases. 
The file has two classes: a Connection Class and a DataBase Class. 
Both Classes allow the user to write SQL queries as normal, and they both provide cnx() and cursor() objects to interact with mysql.connector as normal. 
They both require host, user, and password arguments. 
I have stored my database credentials in a config.py file that is not included in this repo. 

## Connection Class
After passing through your database credentials, the Connection Class allows you to easily query your database connection. 
You can also take advantage of the .add_DB and .drop_DB methods. Both take a database name as an argument. 

## DataBase Class
The DataBase Class is a sub-class of Connection. 
After instantiating a DataBase Class object, the user can us the .query() method to query the database with typical SQL code.
The user can also use the .cnx() or .cursor() methods. 
The .create_table() method allows the user to create a table with a SQL query with some built-in exception handling. 

## Pandas integration
The .query_to_df() method belonging to the DataBase Class will automatically create a Pandas DataFrame from a SQL query. 
The .table_fromDF() method takes in a DataFrame as an argument, and it will create a table in your database from that DataFrame without any additional SQL code. 
This requires a dictionary (included in the source code file) that matches SQL datatypes to numpy datatypes (used by Pandas).
For now, only integers, varchar strings, and datetime datatypes are supported. 
You can also specify the primary key column with an optional argument.
The .insert_fromDf will insert data from a DataFrame into a specified database table. 
.insert_fromDf_iteration will do the same, but it iterates through each indivdual record and prints it so you can track your progress. 




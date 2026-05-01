Inlaw Initial Design
===================

Please read src/inlaw.py and src/dbtable.py

I would like to have an InLaw class, as described in the InLaw link file above, become a command-line tool.

The command-line tool should understand how to look for a local.env file. It should also import the DBTable class so that I can use DBTable objects inside my InLaw tests.

The tool should accept a directory name as an argument. If no directory is provided, it should look in the current directory. It should then load all of the InLaw tests from that directory.

I also want the tool to look for a .env file containing the information needed to create a SQLAlchemy engine connection. It should check both:
	•	the local directory
	•	the parent directory

Once it finds the appropriate .env configuration, it should automatically connect to the database defined there and attempt to run the InLaw tests against that database.

The env parameter should have prefix of InLaw for the variables, and it should support the standard parameter names past that for sqlalchemy connections.

I also want this to become a python package on pypi such that you can use pip to install locally and get the "inlaw" executable in your project. 

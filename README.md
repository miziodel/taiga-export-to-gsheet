# taiga-export-to-gsheet

Script that get epics and stories data from a taiga project endpoints, and after cleaning and merging the data into a single table, insert such table in a google spreadsheet  based on a report template.

## Before you run the script

1. Open the roadmap.py script and adjust all the configurations;
2. create the credentials.json of the service account that will be used to access google drive
3. give proper access to the service account on the google drive folder and report template

# After the script has run

If everything was ok, you need to access the newly created report and adjust pivot tables configuration as needed:

1. check ranges
2. check diagrams ranges and displayed series
3. make sure the pivot filters are not hiding information (i.e. check the total points amount in taiga project and in the report)

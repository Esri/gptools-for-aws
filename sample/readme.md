

# GP tools for AWS Sample 1

This is the same hive sample included in [GIS Tools for Hadoop](https://github.com/Esri/gis-tools-for-hadoop/tree/master/samples), but the script has beem modified with minor changes to work on AWS EMR.


## Features
* Load local GIS Tools for hadoop libs
* Create a table from a csv file
* Create a table from a json file
* Execute a point in polygon query
* Write query result to S3


## Instructions

1. Fork and then clone the repo. 
2. Upload the data files to a location in S3.
3. Modify the HQL script to reflect the data location in S3, then upload it to S3 as well.
4. Run and try the samples script, make sure to point to a non-existent directory in S3 for your output(EMR will create this directory before using it, you shouldn't create it before hand).

## Requirements

* Setup GP Tools for AWS first

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).


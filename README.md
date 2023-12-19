# Match Probabilities for Donors from Transactions Data Import

## Overview
This repository contains a Python script that provides a supplementary feature to aid in detecting whether donors listed in source data from third-party donation platforms already exist in HRI's database or not. Specifically, the script outputs a match probability for each donor's constituent ID found in HRI's database. Existing donors naturally have high probabilities whereas new donors are likely to have very low probabilities or even left as empty, indicating no possible matches.

The match probabilities are calculated via probabilistic entity linkage that implements Fellegi-Sunter model. In the context of this script, probabilities less than 70% are considered not good enough and vice versa. In the case of the former, then, manual verification and fix, if applicable, needs to be carried out.

## Features
- **Database Interaction**: Connect to SQL Server and run queries.
- **SharePoint Integration**: Retrieve files from SharePoint directories.
- **Data Processing**: Use Pandas and Numpy for data transformation.
- **DuckDB and Splink Integration**: For data linking and probabilistic matching.
- **Azure Key Vault**: Secure credential management.

## Matching Criteria
Match criteria based on below columns.
-	`msnfp_customeridname` a.k.a. Donor
- `emailaddress1` a.k.a. Email
- `msnfp_originatingcampaignidname` a.k.a. Originating Campaign
- `msnfp_designationidname` a.k.a. Primary Designation
- `msnfp_appealidname` a.k.a. Appeal
- `msnfp_paymenttypecodename` a.k.a. Payment Type
- `msnfp_transaction_paymentmethodidname` a.k.a. Payment Method

Hard exact match for the below columns.
-	`msnfp_appealidname` a.k.a. Appeal
-	`msnfp_transaction_paymentmethodidname` a.k.a. Payment Method

Softer degree of match (still made to be quite strict but just not have to be exact match) for the below columns. As reference, their respective distance functions are also listed.
- `msnfp_customeridname` a.k.a. Donor
  - Jaro distance
- `emailaddress1` a.k.a. Email
  - Levenshtein distance
- `msnfp_originatingcampaignidname` a.k.a. Originating Campaign
  - Jaro-Winkler distance
- `msnfp_designationidname` a.k.a. Primary Designation
  - Jaro-Winkler distance
- `msnfp_paymenttypecodename` a.k.a. Payment Type
  - Jaro distance

## Prerequisites
- Python 3.x
- Libraries: os, pandas, numpy, io, calendar, datetime, time, pyodbc, splink, azure-identity, azure-keyvault-secrets, shareplum

## Configuration
- Set environment variables for Intel MKL and OpenMP to prevent thread oversubscription.
- Store and retrieve credentials securely using Azure Key Vault.
- Configure SharePoint site and folder paths for data access.

## Usage
- **Database Connection**: Utilize helper functions to connect to SQL Server.
- **Query Execution**: Run complex SQL queries and handle exceptions.
- **SharePoint File Retrieval**: Download files from SharePoint for processing.
- **Dataframe Operations**: Perform data manipulations and preparations using Pandas.
- **Match Probability Calculation**: Implement DuckDB and Splink for data comparison and matching.
- **File Uploading**: Upload processed data back to SharePoint.

## Repository Structure
 ```bash
 /activecampaign
|   Data Import & Donor Prob Matching - Local Version    # To run the script locally
│   README.md
│   script.py    # Main Python script
```

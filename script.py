import os
import pandas as pd
# pd.set_option("display.max_rows", 500)
# pd.set_option("display.max_columns", 500)
import numpy as np
import glob
from io import (StringIO, BytesIO)
import calendar
import datetime
import time
import pyodbc
import yaml
from splink.duckdb.linker import DuckDBLinker
import splink.duckdb.comparison_library as cl
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from shareplum import (Site, Office365)
from shareplum.site import Version

# Limit Intel MKL and OpenMP to single-threaded execution to prevent thread oversubscription
# Oversubscription can cause code execution for parallelization to be stuck
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_DYNAMIC"] = "FALSE"

# Helper function to connect to server
def connect_to_server(server, username, password):
    connection = pyodbc.connect( 
        Trusted_Connection = "No",
        Authentication = "ActiveDirectoryPassword",
        UID = username,
        PWD = password,
        Driver = "{ODBC Driver 17 for SQL Server}",
        Server = server,
        Encrypt = "yes"
    )
    
    return connection

# Helper function to query database
def run_SQL_query(query):
    while True:
        try:
            query_result = \
            cursor.execute(query).fetchall()
        except pyodbc.Error as error:
            if error.args[0] == "08S01": 
                connection.close()
                connection = connect_to_server(server)
                cursor = connection.cursor()
            try:
                query_result = \
                cursor.execute(query).fetchall()
            except: pass
        if query_result != np.nan: break
    
    return query_result  
    
# Helper function to run individual query 
def run_window_SQL_query_for_match_probs(donor):
    query = \
    """
    SELECT T.msnfp_amount, T.msnfp_bookdate, 
           T.msnfp_customeridname, C.emailaddress1, 
           T.msnfp_originatingcampaignidname, 
           T.msnfp_designationidname, 
           T.msnfp_transaction_paymentscheduleidname, 
           T.msnfp_appealidname, T.msnfp_dataentrysourcename, 
           T.msnfp_packageidname, T.msnfp_ccbrandcodename, 
           T.msnfp_paymenttypecodename, 
           T.msnfp_transaction_paymentmethodidname, 
           T.transactioncurrencyidname, C.msnfp_constituentnumber,
           C.hri_alternativeconstituentnumber
    FROM dbo.msnfp_transaction AS T JOIN dbo.contact AS C 
    ON T.msnfp_customerid = C.contactid
    WHERE
    """ 
    if df_transaction["Email"].iloc[donor] is not np.nan:
        if "'" in df_transaction["Donor"].iloc[donor]:
            query += " T.msnfp_customeridname LIKE '%" \
                     + df_transaction["Donor"].iloc[donor].replace("'", "''") + "%' OR" \
                     + " C.emailaddress1 = '" + df_transaction["Email"].iloc[donor].lower() + "' OR"
        if "'" not in df_transaction["Donor"].iloc[donor]:
            query += " T.msnfp_customeridname LIKE '%" \
                     + df_transaction["Donor"].iloc[donor] + "%' OR " \
                     + "C.emailaddress1 = '" + df_transaction["Email"].iloc[donor].lower() + "' OR"   
    if df_transaction["Email"].iloc[donor] is np.nan:
        if "'" in df_transaction["Donor"].iloc[donor]:
            query += " T.msnfp_customeridname LIKE '%" \
                     + df_transaction["Donor"].iloc[donor].replace("'", "''") + "%' OR"
        if "'" not in df_transaction["Donor"].iloc[donor]:
            query += " T.msnfp_customeridname LIKE '%" \
                     + df_transaction["Donor"].iloc[donor] + "%' OR "
    query = query[:-3]
    query_result = run_SQL_query(query)
    _df_query_result = pd.DataFrame.from_records(query_result, 
                                                 columns = [i[0] for i in cursor.description])
    
    return _df_query_result

# Helper function to parallelize donor query
def get_df_for_match_probs(iterator, df_transaction):
    _df_query_result = Parallel(n_jobs = 5, verbose = 0)(
        delayed(run_window_SQL_query_for_match_probs)(i) for i in iterator
        )
    
    return _df_query_result
                
# Helper function to calculate match probability cf. existing data in database
def run_match_prob(i):
    # Initial init
    match_prob = 0
    cons_ID = 0
    
    df_single_donor = df_temp[df_temp["msnfp_customeridname"].str.lower().str.contains(df_transaction["Donor"][i].lower())] \
                             .sort_values(by = ["msnfp_bookdate"], ascending = False)
    if len(df_single_donor) == 1:
        try:
            df_single_donor = df_temp[df_temp["emailaddress1"].str.contains(df_transaction["Email"][i], na = False)] \
                                     .sort_values(by = ["msnfp_bookdate"], ascending = False)     
        except:
            pass
    df_single_donor["unique_id"] = [f"placeholder_id_{index}" for index in range(len(df_single_donor))]
    if len(df_single_donor) == 1:
        match_prob = np.nan
        cons_ID = np.nan
    if len(df_single_donor) > 1:
        linker = DuckDBLinker(df_single_donor, settings, set_up_basic_logging = False)
        linker.estimate_probability_two_random_records_match(deterministic_rules, recall = 1)
        linker.estimate_u_using_random_sampling(max_pairs = 1e6)
        try:
            linkage = linker.predict(threshold_match_probability = None)
            df_linkage = linkage.as_pandas_dataframe()
            df_linkage = df_linkage[df_linkage["unique_id_l"] == "placeholder_id_0"].sort_values(by = ["match_probability"], \
                                                                                                 ascending = False)
            if len(df_linkage[~(df_linkage["msnfp_customeridname_l"].str.replace(" ", "").str.lower() \
                                != df_linkage["msnfp_customeridname_r"].str.replace(" ", "").str.lower())]) > 0:
                df_linkage = df_linkage[~(df_linkage["msnfp_customeridname_l"].str.replace(" ", "").str.lower() \
                                          != df_linkage["msnfp_customeridname_r"].str.replace(" ", "").str.lower())]    
            if len(df_linkage[~(df_linkage["msnfp_customeridname_l"].str.replace(" ", "").str.lower() \
                                != df_linkage["msnfp_customeridname_r"].str.replace(" ", "").str.lower())]) == 0:
                df_linkage = df_linkage[~(df_linkage["emailaddress1_l"].str.replace(" ", "").str.lower() \
                                          != df_linkage["emailaddress1_r"].str.replace(" ", "").str.lower())]        
            match_prob = df_linkage.iloc[0, :]["match_probability"]
            cons_ID = df_single_donor[df_single_donor["unique_id"] \
                                      == df_linkage.iloc[0, :]["unique_id_r"]]["msnfp_constituentnumber"].iloc[0]
        except:
            match_prob = np.nan
            cons_ID = np.nan
        
    return match_prob, cons_ID

# Helper function to parallelize calculating match probabilities
def get_all_match_probs(iterator):
    list_match_prob = [] 
    list_cons_ID = []
    match_prob, cons_ID = Parallel(n_jobs = 5, verbose = 0)(
        delayed(run_match_prob)(i) for i in iterator
        )
    list_match_prob.extend(match_prob)
    list_cons_ID.extend(cons_ID)
    
    return list_match_prob, list_cons_ID
    
# Init date vars
recon_month, recon_year = datetime.datetime.now().strftime("%B"), datetime.datetime.now().strftime("%Y")
start_date = datetime.datetime(datetime.datetime.now().year, \
                               list(calendar.month_name).index(recon_month.lower().capitalize()), \
                               1).date()
end_date = start_date.replace(day = 28) \
           + datetime.timedelta(days = 5) \
           - datetime.timedelta(days = (start_date.replace(day = 28) + datetime.timedelta(days = 5)).day)

# Init vars for match probs
index_step = 10
match_threshold = 0.7
settings = {"link_type": "dedupe_only",
            "blocking_rules_to_generate_predictions": 
                ["l.msnfp_customeridname = r.msnfp_customeridname",
                 "l.emailaddress1 = r.emailaddress1",
                 "l.msnfp_originatingcampaignidname = r.msnfp_originatingcampaignidname",
                 "l.msnfp_designationidname = r.msnfp_designationidname",
                 "l.msnfp_appealidname = r.msnfp_appealidname",
                 "l.msnfp_paymenttypecodename = r.msnfp_paymenttypecodename",
                 "l.msnfp_transaction_paymentmethodidname = r.msnfp_transaction_paymentmethodidname"],
            "comparisons": 
                [cl.jaro_at_thresholds("msnfp_customeridname", [0.9, 0.7], term_frequency_adjustments = True), 
                 cl.levenshtein_at_thresholds("emailaddress1"), 
                 cl.jaro_at_thresholds("msnfp_paymenttypecodename", [0.9, 0.7], term_frequency_adjustments = True), 
                 cl.jaro_winkler_at_thresholds("msnfp_originatingcampaignidname", [0.9, 0.7], term_frequency_adjustments = True), 
                 cl.jaro_winkler_at_thresholds("msnfp_designationidname", [0.9, 0.7], term_frequency_adjustments = True)]}
deterministic_rules = ["l.msnfp_originatingcampaignidname = r.msnfp_originatingcampaignidname", 
                       "l.msnfp_designationidname = r.msnfp_designationidname", 
                       "l.msnfp_appealidname = r.msnfp_appealidname", 
                       "l.emailaddress1 = r.emailaddress1"]

# Init vars to connect to server & database
vault_name = ""
url_vault = f"https://{vault_name}.vault.azure.net/"
client_vault = SecretClient(vault_url = url_vault, credential = DefaultAzureCredential())
username = client_vault.get_secret("username_SharePoint").value
password = client_vault.get_secret("password_SharePoint").value
server = "HRICRM-PROD.CRM6.DYNAMICS.COM"
connection = connect_to_server(server, username, password)
cursor = connection.cursor()

# Init vars to access all relevant Fundraising SharePoint folders
url_sharepoint = "https://heartresearchinstitute.sharepoint.com"
site_sharepoint = "sites/HRICRM"
auth_sharepoint = Office365(url_sharepoint, username = username, password = password) \
                  .GetCookies()
df_CRM_codes = Site(f"{url_sharepoint}/{site_sharepoint}", version = Version.v365, 
                    authcookie = auth_sharepoint) \
               .Folder("Shared Documents/Fundraising Operations/Single Giving") \
               .get_file("FRO_CRM_and_Finance-Activity_and_Coding_Reference.xlsb")
df_CRM_codes = BytesIO(df_CRM_codes)
df_CRM_codes = pd.read_excel(df_CRM_codes)
for i in ["Braintree", "Benevity", "Good2Give", "Karma Currency", "UK Charities"]:
    globals()[f"path_sharepoint_folder_{i}"] = f"Shared Documents/Fundraising Operations/Single Giving/{i}/" \
                                               + f"Downloaded Data/{recon_month} {recon_year}"
    globals()[f"folder_sharepoint_{i}"] = Site(f"{url_sharepoint}/{site_sharepoint}", version = Version.v365, 
                                               authcookie = auth_sharepoint) \
                                          .Folder(globals()[f"path_sharepoint_folder_{i}"])
    globals()[f'df_{i.replace(" ", "")}'] = globals()[f"folder_sharepoint_{i}"].get_file(
        globals()[f"folder_sharepoint_{i}"].files["Name"][0]
    )
    globals()[f'df_{i.replace(" ", "")}'] = StringIO(globals()[f'df_{i.replace(" ", "")}'].decode("utf-8"))
    if (i == "Braintree") | (i == "Karma Currency"):
        globals()[f'df_{i.replace(" ", "")}'] = pd.read_csv(globals()[f'df_{i.replace(" ", "")}'])
    if i == "Good2Give":
        globals()[f'df_{i.replace(" ", "")}'] = pd.read_csv(globals()[f'df_{i.replace(" ", "")}'], 
                                                            skiprows = 2, skipfooter = 2, engine = "python")
    if i == "Benevity":
        globals()[f'df_{i.replace(" ", "")}'] = pd.read_csv(globals()[f'df_{i.replace(" ", "")}'], 
                                                            skiprows = 11, skipfooter = 4, engine = "python")
    if i == "UK Charities":
        globals()[f'df_{i.replace(" ", "")}'] = pd.read_csv(globals()[f'df_{i.replace(" ", "")}'])
path_sharepoint_folder_Insight_weekly = f"Shared Documents/Fundraising Operations/Single Giving/Insight/" \
                                        + f"Weekly Report Folder/{recon_month} {recon_year}"
folder_sharepoint_Insight_weekly = Site(f"{url_sharepoint}/{site_sharepoint}", version = Version.v365, 
                                        authcookie = auth_sharepoint) \
                                   .Folder(path_sharepoint_folder_Insight_weekly)
df_Insight_weekly = pd.DataFrame()
for i in folder_sharepoint_Insight_weekly.files():
    _df_Insight_weekly = folder_sharepoint_Insight_weekly.get_file(i["Name"])
    _df_Insight_weekly = BytesIO(_df_Insight_weekly)
    _df_Insight_weekly = pd.read_excel(_df_Insight_weekly)
    _df_Insight_weekly = pd.concat(
        [
            df_Insight_weekly,
            _df_Insight_weekly
        ],
        axis = 0
    )

# Braintree
df_Braintree["Donor"] = df_Braintree["Customer First Name"].str.capitalize() + " " + \
                        df_Braintree["Customer Last Name"].str.capitalize()
df_Braintree["Donor"] = df_Braintree["Donor"].str.title()
df_Braintree["Payment Method"] = df_Braintree["Card Type"] + " - " + df_Braintree["Last Four of Credit Card"].astype("str")
df_Braintree = df_Braintree[["Settlement Date", "Transaction ID", "Subscription ID", "Amount Submitted For Settlement",
                             "Currency ISO Code", "Campaign Id",
                             "Donor", "Customer First Name", "Customer Last Name",
                             "Customer Email", "Customer Phone", "Payment Method", "Card Type"]]
df_Braintree["Originating Campaign"] = \
np.select([(df_Braintree["Subscription ID"].isnull()) & (df_Braintree["Currency ISO Code"] == "AUD"),
           (df_Braintree["Subscription ID"].isnull()) & (df_Braintree["Currency ISO Code"] == "NZD"),
           (df_Braintree["Subscription ID"].isnull()) & (df_Braintree["Currency ISO Code"] == "GBP"),
           (df_Braintree["Subscription ID"].notna()) & (df_Braintree["Currency ISO Code"] == "AUD"),
           (df_Braintree["Subscription ID"].notna()) & (df_Braintree["Currency ISO Code"] == "NZD"),
           (df_Braintree["Subscription ID"].notna()) & (df_Braintree["Currency ISO Code"] == "GBP")],
          ["AUSG-DGAQ-HR", "NZSG-DGAQ-HR", "UKSG-DGAQ-HR", "AURG-DGAQ-HR", "NZRG-DGAQ-HR", "UKRG-DGAQ-HR"])
df_Braintree["Originating Campaign"] = \
np.select([df_Braintree["Campaign Id"].str.contains("tax", na = False),
           df_Braintree["Campaign Id"].str.contains("xmas", na = False)],
          ["AUSG-DMRN-HR-" + df_Braintree["Campaign Id"].str[-2:] + "02-D1",
           "AUSG-DMRN-HR-" + df_Braintree["Campaign Id"].str[-2:] + "04-W2"],
          default = df_Braintree["Originating Campaign"])
df_Braintree["Primary Designation"] = \
[df_CRM_codes[df_CRM_codes["CampaignID"] == df_Braintree["Originating Campaign"][i]]["DesignationID (Finance GL Codes)"] \
 .values[0].split()[0] \
 for i in range(len(df_Braintree["Originating Campaign"]))]
df_Braintree["Appeal"] = \
[df_CRM_codes[df_CRM_codes["CampaignID"] == df_Braintree["Originating Campaign"][i]]["AppealID"].values[0] \
 for i in range(len(df_Braintree["Originating Campaign"]))]
df_Braintree["Appeal"] = [i[0] + " " + i[1] for i in df_Braintree["Appeal"].str.split()]
df_Braintree["Payment Type"] = "External Credit/Debit"
df_Braintree["Settlement Date"] = pd.to_datetime(df_Braintree["Settlement Date"], yearfirst = True).dt.date
df_Braintree[["Data Entry Source", "Package"]] = "Online", "Online"
df_Braintree["Data Entry Reference"] = \
np.select([(df_Braintree["Currency ISO Code"] == "AUD") & (df_Braintree["Originating Campaign"] == "AUSG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "NZD") & (df_Braintree["Originating Campaign"] == "NZSG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "GBP") & (df_Braintree["Originating Campaign"] == "UKSG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "AUD") & (df_Braintree["Originating Campaign"] == "AURG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "NZD") & (df_Braintree["Originating Campaign"] == "NZRG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "GBP") & (df_Braintree["Originating Campaign"] == "UKRG-DGAQ-HR"), 
           (df_Braintree["Currency ISO Code"] == "AUD") & (df_Braintree["Originating Campaign"] == "AUSG-DMRN-HR-" \
                                                           + df_Braintree["Campaign Id"].str[-2:] + "02-D1"),
           (df_Braintree["Currency ISO Code"] == "AUD") & (df_Braintree["Originating Campaign"] == "AUSG-DMRN-HR-" \
                                                           + df_Braintree["Campaign Id"].str[-2:] + "04-W2")],
          [f'Braintree AU Online OO {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree NZ Online OO {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree UK Online OO {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree AU Online RG {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree NZ Online RG {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree UK Online RG {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}', 
           f'Braintree AU Online OO Tax Appeal {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}',
           f'Braintree AU Online OO Xmas Appeal {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}'])
df_Braintree["Data Entry Reference"] = np.select([df_Braintree["Amount Submitted For Settlement"] < 0],
                                                 [df_Braintree["Data Entry Reference"].astype("str") + " Refund"],
                                                 default = df_Braintree["Data Entry Reference"])
df_Braintree["Identifier"] = "Braintree - " + df_Braintree["Transaction ID"]
df_Braintree = df_Braintree.rename(columns = {"Settlement Date": "Book Date", "Amount Submitted For Settlement": "Amount",
                                              "Currency ISO Code": "Currency", "Customer Email": "Email",
                                              "Customer First Name": "First Name", "Customer Last Name": "Last Name",
                                              "Customer Phone": "Phone", "Card Type": "Card Brand",
                                              "Campaign Id": "donation_type"})

# Benevity
# Sum up below 2 columns to match amt received in NABGEN
df_Benevity["Total Donation"] = df_Benevity["Total Donation to be Acknowledged"].str.replace(",", "").astype("float") \
                                + df_Benevity["Match Amount"]
df_Benevity = df_Benevity[["Company", "Donor First Name", "Donor Last Name", "Email",
                           "Address", "City", "State/Province", "Postal Code", "Match Amount",
                           "Total Donation", "Transaction ID", "Donation Frequency", "Currency"]]
df_Benevity["Donor"] = \
df_Benevity["Donor First Name"].str.capitalize() + " " + df_Benevity["Donor Last Name"].str.capitalize()
df_Benevity["Donor"] = df_Benevity["Donor"].str.title()
df_Benevity["Donor"] = np.select([df_Benevity["Donor"].str.contains("Not shared")],
                                 [df_Benevity["Company"].astype("str") + " (Anonymous Employee)"],
                                 default = df_Benevity["Donor"])
for i in ["Donor First Name", "Donor Last Name", "Address", "City", "State/Province", "Postal Code"]:
    df_Benevity[i] = df_Benevity[i].astype("str")
    df_Benevity[i] = [df_Benevity[i][j].replace(df_Benevity[i][j], "") if "Not shared" \
                      in df_Benevity[i][j] else df_Benevity[i][j] for j in range(len(df_Benevity[i]))]
df_Benevity["Identifier"] = "Benevity - " + df_Benevity["Transaction ID"].astype("str")
df_Benevity[["Payment Type", "Package", "Data Entry Source"]] = "Other", "Benevity", "Direct Deposit"
df_Benevity["Originating Campaign"] = np.select([df_Benevity["Donation Frequency"] == "One Time",
                                                 df_Benevity["Donation Frequency"] == "Recurring"],
                                                ["AUSG-DGAQ-CM", "AUMG-COAQ-HR-WP"])
df_Benevity["Primary Designation"] = np.select([df_Benevity["Donation Frequency"] == "One Time",
                                                df_Benevity["Donation Frequency"] == "Recurring"],
                                               ["FA-SGCF-1001", "FA-PHWG-1001"])
df_Benevity["Appeal"] = np.select([df_Benevity["Donation Frequency"] == "One Time",
                                   df_Benevity["Donation Frequency"] == "Recurring"],
                                  ["SG DIGI", "PHIL CORP"])
df_Benevity["Ref"] = \
np.select([df_Benevity["Match Amount"] == 0, df_Benevity["Match Amount"] != 0],
          ["AUSOGF",
           "AUSOGF - " + df_Benevity["Company"].astype("str") + " " + df_Benevity["Currency"].astype("str") + " " \
           + df_Benevity["Match Amount"].astype("str") + " Matched Gift"])
df_Benevity["Data Entry Reference"] = f'AUSOGF {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}'
df_Benevity["Data Entry Source"] = "Direct Deposit"
df_Benevity = df_Benevity.rename(columns = {"Company": "Organization Name",
                                            "Donor First Name": "First Name", "Donor Last Name": "Last Name",
                                            "Address": "Street 1", "Postal Code": "ZIP/Postal Code",
                                            "Total Donation": "Amount",
                                            "Donation Frequency": "donation_type"}) \
                         .drop(labels = ["Match Amount"], axis = 1)

# Good2Give
df_Good2Give = df_Good2Give.dropna(subset = ["Donation Confirmation Number"])
# 5% accosted to admin fees
df_Good2Give["Donation Amount"] = 0.95 * \
                                  df_Good2Give["Donation Amount"].str.replace("$", "", regex = False).astype("float")
df_Good2Give = df_Good2Give[["Good2Give Donor Id", "Donor Firstname", "Donor Lastname", "Donation Amount",
                             "Employer Name", "Donor Email", "Donation Type", "Donation Confirmation Number"]]
df_Good2Give["Donor"] = \
df_Good2Give["Donor Firstname"].str.capitalize() + " " + df_Good2Give["Donor Lastname"].str.capitalize()
df_Good2Give["Donor"] = df_Good2Give["Donor"].str.title()
df_Good2Give[["Currency", "Originating Campaign", "Primary Designation", "Appeal"]] = \
"AUD", "AUMG-COAQ-HR-WP", "FA-PHWG-1001", "PHIL CORP"
df_Good2Give["Identifier"] = "Good2Give - " + df_Good2Give["Donation Confirmation Number"].astype("str")
df_Good2Give["Payment Type"] = "Other"
df_Good2Give["Data Entry Source"] = "Direct Deposit"
df_Good2Give["Ref"] = \
np.select([df_Good2Give["Donation Type"] == "Workplace Giving", \
           (df_Good2Give["Donation Type"] == "Employer Matched") | (df_Good2Give["Donation Type"] == "Match")], \
          ["Good2Give Donor ID " + df_Good2Give["Good2Give Donor Id"].astype("int").astype("str"), \
           "Good2Give Donor ID " + df_Good2Give["Good2Give Donor Id"].astype("int").astype("str") + " - Employer Matched"])
df_Good2Give["Data Entry Reference"] = f'Good2Give {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}'
df_Good2Give = df_Good2Give.rename(columns = {"Donor Firstname": "First Name", "Donor Lastname": "Last Name",
                                              "Donation Amount": "Amount", "Donor Email": "Email",
                                              "Donation Confirmation Number": "Transaction ID",
                                              "Employer Name": "Organization Name",
                                              "Donation Type": "donation_type"}) \
                           .drop(labels = ["Good2Give Donor Id"], axis = 1)

# Karma Currency (i.e. GoodCompany)
df_KarmaCurrency["Donor"] = df_KarmaCurrency["First Name"].str.capitalize() + " " + \
                          df_KarmaCurrency["Last Name"].str.capitalize()
df_KarmaCurrency["Donor"] = df_KarmaCurrency["Donor"].str.title()
df_KarmaCurrency["State"] = df_KarmaCurrency["State"].str.upper()
df_KarmaCurrency = df_KarmaCurrency[["Date", "Amount", "Site", "Type", "Recurrence",
                                     "Donor", "First Name", "Last Name", "Email", "State", "Postal Address", "Suburb", "Post Code",
                                     "Transaction Reference"]]
df_KarmaCurrency["Originating Campaign"] = np.select([(df_KarmaCurrency["Type"] == "Payroll Giving") | (df_KarmaCurrency["Type"] == "Matched Giving")],
                                                     ["AUMG-COAQ-HR-WP"], default = "AUSG-DGAQ-CM")
df_KarmaCurrency["Primary Designation"] = np.select([(df_KarmaCurrency["Type"] == "Payroll Giving") | (df_KarmaCurrency["Type"] == "Matched Giving")],
                                                    ["FA-PHWG-1001"], default = "FA-SGCF-1001")
df_KarmaCurrency["Appeal"] = np.select([(df_KarmaCurrency["Type"] == "Payroll Giving") | (df_KarmaCurrency["Type"] == "Matched Giving")],
                                       ["PHIL CORP"], default = "SG DIGI")
df_KarmaCurrency["Date"] = pd.to_datetime(df_KarmaCurrency["Date"], yearfirst = True).dt.date
df_KarmaCurrency["Identifier"] = "KarmaCurrency - " + df_KarmaCurrency["Transaction Reference"].astype("str")
df_KarmaCurrency[["Payment Type", "Package", "Data Entry Source", "Currency"]] = "Other", "KarmaCurrency/Karma Currency", "Direct Deposit", "AUD"
df_KarmaCurrency["Data Entry Reference"] = f'KarmaCurrency Fd {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}'
df_KarmaCurrency["Ref"] = df_KarmaCurrency["Transaction Reference"]
df_KarmaCurrency = df_KarmaCurrency.rename(columns = {"Date": "Book Date", "State": "State/Province", "Site": "Organization Name",
                                                      "Postal Address": "Street 1", "Suburb": "City", "Post Code": "ZIP/Postal Code",
                                                      "Transaction Reference": "Transaction ID", "Recurrence": "donation_type"})

# Charities Trust UK
df_UKCharities["Donor"] = df_UKCharities["First Name"].str.capitalize() + " " + df_UKCharities["Last Name"].str.capitalize()
df_UKCharities["Donor"] = df_UKCharities["Donor"].str.title()
df_UKCharities["Identifier"] = "Charities Trust UK - " + df_UKCharities["PGA Donor Ref Number"].astype("str")
df_UKCharities = df_UKCharities[["PGA Donor Ref Number", "Employer Name", "First Name", "Last Name", "Donor", "Donor Email", "Donor Tel No", 
                                 "Address 1", "Address 2", "Address 3", "Postcode", 
                                 "Total Donation to Charity", "Donor Preference Post", "Donor Preference Email", 
                                 "Donor Preference Phone", "Donor Preference SMS", "Identifier"]]
df_UKCharities[["Currency", "Originating Campaign", "Primary Designation", "Appeal", "Package"]] = \
"GBP", "UKMG-COAQ-HR-WP", "FG-PHWG-1001", "PHIL CORP", "Charities Trust UK"
df_UKCharities["Payment Type"] = "Other"
df_UKCharities["Data Entry Source"] = "Direct Deposit"
df_UKCharities["Data Entry Reference"] = f'Charities Trust UK {str(start_date).replace("-", "")}_{str(end_date).replace("-", "")}'
df_UKCharities["Ref"] = "Charities Trust UK " + df_UKCharities["PGA Donor Ref Number"].astype("str")
df_UKCharities["donation_type"] = "Recurring"
df_UKCharities = df_UKCharities.rename(columns = {"PGA Donor Ref Number": "Transaction ID", "Employer Name": "Organization Name", 
                                                  "Address 1": "Street 1", "Address 2": "Street 2", "Address 3": "Street 3", 
                                                  "Postcode": "ZIP/Postal Code", "Donor Tel No": "Phone", 
                                                  "Total Donation to Charity": "Amount", "Donor Email": "Email"})

# Insight - weekly
df_Insight_weekly = df_Insight_weekly[["Week Ending", "Bank Date", "Insight WP Number", "Customer Id", "First name", "SurName", 
                                       "Address1", "Suburb", "State", "Post Code", "Telephone", "Email Address", 
                                       "Donation Value $", "Sale Type"]]
df_Insight_weekly["Donor"] = df_Insight_weekly["First name"].str.capitalize() \
                             + " " + df_Insight_weekly["SurName"].str.capitalize()
df_Insight_weekly["Donor"] = df_Insight_weekly["Donor"].str.title()
df_Insight_weekly[["Data Entry Source", "Originating Campaign", "Primary Designation", "Appeal", "Package", \
                   "Payment Type", "Ref", "Country/Region", "Currency"]] = \
"TM", "AUSG-TMAQ-IN-DONATE", "FA-SGLR-1001", "SG LOTT/RAFF", "DONATIONS", "Other", "Receipted by Insight", "Australia", "AUD"
df_Insight_weekly["Data Entry Reference"] = "Westpac_Insight OO " \
                                            + df_Insight_weekly["Week Ending"].astype("str").str.replace("-", "")
df_Insight_weekly["Identifier"] = "Insight - " + df_Insight_weekly["Customer Id"]
df_Insight_weekly["Post Code"] = np.select([df_Insight_weekly["Post Code"].astype("str").str.len() < 4], 
                                           ["0" + df_Insight_weekly["Post Code"].astype("str")],
                                           default = df_Insight_weekly["Post Code"].astype("str"))
df_Insight_weekly = df_Insight_weekly.rename(columns = {"Bank Date": "Book Date", "Insight WP Number": "hri_alternativeconstituentnumber",
                                                        "First name": "First Name", "SurName": "Last Name", "Address1": "Street 1", 
                                                        "Suburb": "City", "State": "State/Province", "Post Code": "ZIP/Postal Code", 
                                                        "Telephone": "Phone", "Email Address": "Email", "Donation Value $": "Amount", 
                                                        "Sale Type": "SG External Program Paymentmethod"}) \
                                     .drop(labels = ["Customer Id"], axis = 1)

# Combine the above dfs
df_transaction = pd.concat(
    [
        df_Braintree,
        df_Benevity,
        df_Good2Give,
        df_KarmaCurrency,
        df_UKCharities,
        df_Insight_weekly
    ],
    axis = 0
)
# Data quality check & transformation when necessary
df_transaction[["Status Reason", "Status"]] = "Completed", "Active"
df_transaction["Status Reason"] = np.select([df_transaction["Amount"] < 0], 
                                            ["Refund"], 
                                            default = df_transaction["Status Reason"])
df_transaction["Anonymous"] = np.select([df_transaction["Donor"].isnull()], 
                                        ["Yes"], 
                                        default = "No")
df_transaction["Country/Region"] = \
np.select([df_transaction["Currency"] == "AUD",
           df_transaction["Currency"] == "NZD",
           df_transaction["Currency"] == "GBP"],
          ["Australia", "New Zealand", "United Kingdom"], 
          default = df_transaction["Country/Region"])
df_transaction["Type"] = "Donation"
df_transaction["Phone"] = df_transaction["Phone"].str.replace(" ", "")
df_transaction["Phone"] = np.select([df_transaction["Phone"].notna()], 
                                    ["0" + df_transaction["Phone"].astype("Int64").astype("str")],
                                    default = df_transaction["Phone"])
df_transaction["ZIP/Postal Code"] = pd.to_numeric(df_transaction["ZIP/Postal Code"], errors = "coerce")
df_transaction["ZIP/Postal Code"] = np.select([df_transaction["ZIP/Postal Code"].astype("Int64").astype("str").str.len() < 4, 
                                               df_transaction["ZIP/Postal Code"].isnull()],
                                              ["0" + df_transaction["ZIP/Postal Code"].astype("Int64").astype("str"), 
                                               None],
                                              default = df_transaction["ZIP/Postal Code"].astype("Int64").astype("str"))
df_transaction["Email"] = df_transaction["Email"].str.lower()
# Anonymous handling unprocessed in previous steps
list_columns = ["Primary Designation", "Originating Campaign", "Appeal", "Donor"]
list_choices = [
    ["FA-SGDM-1990", "FN-SGDM-1990", "FG-SGDM-1990"],
    ["AUSG-DMAQ-UN", "NZSG-DMAQ-UN", "UKSG-DMAQ-UN"],
    ["SG GENERAL", "SG GENERAL", "SG GENERAL"],
    ["Anonymous Anonymous", "Anonymous NZ", "Anonymous UK"]
]
for i in range(len(list_columns)):
    choice_1, choice_2, choice_3 = list_choices[i][0], list_choices[i][1], list_choices[i][2],
    df_transaction[list_columns[i]] = \
    np.select([((df_transaction["Anonymous"] == "Yes") & (df_transaction["Currency"] == "AUD") \
                & (df_transaction["Donor"].isnull()) & (df_transaction["Primary Designation"].isnull()) \
                & (df_transaction["Originating Campaign"].isnull()) & (df_transaction["Appeal"].isnull())),          
               ((df_transaction["Anonymous"] == "Yes") & (df_transaction["Currency"] == "NZD") \
                & (df_transaction["Donor"].isnull()) & (df_transaction["Primary Designation"].isnull()) \
                & (df_transaction["Originating Campaign"].isnull()) & (df_transaction["Appeal"].isnull())),       
               ((df_transaction["Anonymous"] == "Yes") & (df_transaction["Currency"] == "GBP") \
                & (df_transaction["Donor"].isnull()) & (df_transaction["Primary Designation"].isnull()) \
                & (df_transaction["Originating Campaign"].isnull()) & (df_transaction["Appeal"].isnull()))], \
              [choice_1, choice_2, choice_3], default = df_transaction[list_columns[i]])
df_transaction["Donor"] = np.select([((df_transaction["Anonymous"] == "Yes") & (df_transaction["Donor"].isnull()) \
                                      & (df_transaction["Currency"] == "AUD")), \
                                     ((df_transaction["Anonymous"] == "Yes") & (df_transaction["Donor"].isnull()) \
                                      & (df_transaction["Currency"] == "NZD")), \
                                     ((df_transaction["Anonymous"] == "Yes") & (df_transaction["Donor"].isnull()) \
                                      & (df_transaction["Currency"] == "GBP"))], \
                                    ["Anonymous Anonymous", "Anonymous NZ", "Anonymous UK"], default = df_transaction["Donor"])

# Get Payment Schedule ID
query_result = run_SQL_query("""
                             SELECT msnfp_customeridname, msnfp_customerid,
                                    msnfp_emailaddress1, msnfp_name, statuscode, 
                                    msnfp_originatingcampaignidname,
                                    msnfp_amount_receipted
                             FROM dbo.msnfp_paymentschedule
                             WHERE msnfp_name LIKE '%Braintree%'
                                   AND statuscode <> '844060001'
                             """)
df_transaction["join_on_payschedID"] = \
np.select([(df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Data Entry Reference"].str.contains("RG"))],
          [df_transaction["Subscription ID"]],
          default = None)
# Store payschedID query result to ensure newly assigned payschedIDs are unique 
df_payschedID = pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])
df_payschedID["join_on_payschedID"] = \
np.select([df_payschedID["msnfp_name"].str.contains("DIGI")], \
          [df_payschedID["msnfp_name"].str.split(" ").str[-1]], \
          default = None)
df_transaction = df_transaction.merge(df_payschedID, on = ["join_on_payschedID"], how = "left") \
                               .drop(labels = ["msnfp_customeridname", "msnfp_customerid", "msnfp_emailaddress1", "statuscode", \
                                               "msnfp_originatingcampaignidname", "msnfp_amount_receipted"], \
                                     axis = 1)
df_transaction = \
pd.concat([df_transaction[df_transaction["Donor"].isin(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description]) \
                                                       [(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])["msnfp_customeridname"].str.lower() \
                                                        .isin(df_transaction[df_transaction["Payment Schedule"].isnull()]["Donor"].str.lower()))]["msnfp_customeridname"])] \
           .merge(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description]) \
                  [(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])["msnfp_customeridname"].str.lower() \
                    .isin(df_transaction[df_transaction["Payment Schedule"].isnull()]["Donor"].str.lower()))][["msnfp_customeridname", "msnfp_name"]] \
                  .rename(columns = {"msnfp_customeridname": "Donor"}), \
                  on = ["Donor"]).drop(labels = ["Payment Schedule"], axis = 1).rename(columns = {"msnfp_name": "Payment Schedule"}), \
           df_transaction[~df_transaction["Donor"].isin(df_transaction[df_transaction["Donor"] \
                                                                      .isin(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description]) \
                                                                            [(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])["msnfp_customeridname"].str.lower() \
                                                                              .isin(df_transaction[df_transaction["Payment Schedule"].isnull()]["Donor"].str.lower()))]["msnfp_customeridname"])] \
                                                        .merge(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description]) \
                                                               [(pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])["msnfp_customeridname"].str.lower() \
                                                                 .isin(df_transaction[df_transaction["Payment Schedule"].isnull()]["Donor"].str.lower()))][["msnfp_customeridname", "msnfp_name"]] \
                                                               .rename(columns = {"msnfp_customeridname": "Donor"}), \
                                                               on = ["Donor"]).drop(labels = ["Payment Schedule"], axis = 1).rename(columns = {"msnfp_name": "Payment Schedule"})["Donor"])]], \
          axis = 0)
df_transaction["Payment Schedule"] = \
np.select([(df_transaction["msnfp_name_x"].notnull()) & (df_transaction["msnfp_name_y"].notnull()) & (df_transaction["msnfp_name_x"]) == (df_transaction["msnfp_name_y"]), \
           (df_transaction["msnfp_name_x"].notnull()) & (df_transaction["msnfp_name_y"].isnull()), \
           (df_transaction["msnfp_name_x"].isnull()) & (df_transaction["msnfp_name_y"].notnull()), \
           (df_transaction["msnfp_name"].notnull())], \
          [df_transaction["msnfp_name_x"], df_transaction["msnfp_name_x"], df_transaction["msnfp_name_y"], df_transaction["msnfp_name"]], \
          default = df_transaction["Payment Schedule"])
df_transaction = df_transaction.drop(labels = ["msnfp_name_x", "msnfp_name_y", "msnfp_name"], axis = 1)
df_transaction["Payment Schedule"] = \
[("DIGI AU Braintree (SubID: " + df_transaction["join_on_payschedID"][i] + " )") \
 if ("Braintree" in df_transaction["Data Entry Reference"][i]) \
    & (True in list(df_payschedID["msnfp_name"].str.contains(str(df_transaction["join_on_payschedID"][i])))) \
 else None \
 for i in range(len(df_transaction))]
df_transaction["Payment Schedule"] = \
np.select([(df_transaction["Currency"] == "NZD") & (df_transaction["Data Entry Reference"].str.contains("Braintree")),
           (df_transaction["Currency"] == "GBP") & (df_transaction["Data Entry Reference"].str.contains("Braintree"))],
          [df_transaction["Payment Schedule"].str.replace("AU", "NZ"),
           df_transaction["Payment Schedule"].str.replace("AU", "UK")],
          default = df_transaction["Payment Schedule"])

# Get & assign the highest matched probability of cons ID for each donor
df_temp = df_transaction[["Amount", "Book Date", "Donor", "Email", "Originating Campaign", "Primary Designation", 
                          "Payment Schedule", "Appeal", "Data Entry Source", "Package", "Card Brand", 
                          "Payment Type", "Payment Method", "Currency", "hri_alternativeconstituentnumber"]] \
                        .rename(columns = \
                               {"Amount": "msnfp_amount", "Book Date": "msnfp_bookdate", 
                                "Donor": "msnfp_customeridname", "Email": "emailaddress1", 
                                "Originating Campaign": "msnfp_originatingcampaignidname", 
                                "Primary Designation": "msnfp_designationidname", 
                                "Payment Schedule": "msnfp_transaction_paymentscheduleidname", 
                                "Appeal": "msnfp_appealidname", "Data Entry Source": "msnfp_dataentrysourcename", 
                                "Package": "msnfp_packageidname", "Card Brand": "msnfp_ccbrandcodename", 
                                "Payment Type": "msnfp_paymenttypecodename", 
                                "Payment Method": "msnfp_transaction_paymentmethodidname", 
                                "Currency": "transactioncurrencyidname"})
list_index = [0]
while list_index[-1] < len(df_transaction):
    list_index.append(list_index[-1] + (len(df_transaction) // index_step))
list_index[-1] = len(df_transaction) - 1
for index in range(len(list_index) - 1):
    iterator = range(list_index[index], list_index[index + 1])
    _df_query_result = get_df_for_match_probs(iterator, df_transaction)
    df_temp = pd.concat(
        [
            df_temp, 
            _df_query_result
        ], 
        axis = 0
    )
iterator = range(len(df_transaction))
list_match_prob, list_cons_ID = get_all_match_probs(iterator)
df_transaction["msnfp_constituentnumber"] = list_conID
df_transaction["match_prob"] = list_match_prob
df_transaction["msnfp_constituentnumber"] = \
np.select([(df_transaction["Donor"] == "Anonymous Anonymous"),
           (df_transaction["Donor"] == "Anonymous NZ"), 
           (df_transaction["Donor"] == "Anonymous UK")],
          ["151334", "150888", "143900"],
          default = df_transaction["msnfp_constituentnumber"])

# Get cons ID for organization donors
query_result = run_SQL_query("""
                             SELECT DISTINCT(acc.msnfp_constituentnumber),
                                    acc.name
                             FROM dbo.account AS acc
                             """)
df_acc_cons_ID = pd.DataFrame.from_records(query_result, columns = [i[0] for i in cursor.description])
for i in list(df_transaction[(df_transaction["Ref"].str.contains("Matched", na = False)) \
                             & (df_transaction["Donor"].str.contains("Anonymous"))].index):
    df_transaction["msnfp_constituentnumber"][i] = df_acc_conID[df_acc_conID["name"] \
                                                                == df_transaction["Organization Name"][i]]["accountnumber"]
    
# Handle incorrectly detected dates, if any
df_transaction["Book Date"] = [pd.to_datetime(pd.to_datetime(i).strftime("%Y-%d-%m")) \
                               if pd.to_datetime(i) > datetime.now() \
                               else pd.to_datetime(i) \
                               for i in df_transaction["Book Date"]]

# Assert data types & mapping
df_transaction["Amount"] = df_transaction["Amount"].astype("float")
df_transaction["Currency"] = df_transaction["Currency"].map({"AUD": "Australian Dollar",
                                                             "NZD": "New Zealand Dollar",
                                                             "GBP": "UK Pound",
                                                             "CAD": "Canadian Dollar"})
    
# Assign a flagger for each donor
df_transaction["donor_flagger"] = \
np.select([(df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].notna()) \
           & (df_transaction["join_on_payschedID"].notna()) & (df_transaction["msnfp_constituentnumber"].notna()) \
           & (df_transaction["match_prob"] < match_threshold),
           (df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].isnull()) \
           & (df_transaction["join_on_payschedID"].isnull()) & (df_transaction["msnfp_constituentnumber"].notna()) \
           & (df_transaction["match_prob"] < match_threshold),
           (df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].isnull()) \
           & (df_transaction["join_on_payschedID"].notna()) & (df_transaction["msnfp_constituentnumber"].isnull()) \
           & (df_transaction["match_prob"].isnull()),
           (df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].isnull()) \
           & (df_transaction["join_on_payschedID"].notna()) & (df_transaction["msnfp_constituentnumber"].notna()) \
           & (df_transaction["match_prob"] < match_threshold),
           (df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].isnull()) \
           & (df_transaction["join_on_payschedID"].notna()) & (df_transaction["msnfp_constituentnumber"].notna()) \
           & (df_transaction["match_prob"] >= match_threshold),
           (df_transaction["Data Entry Reference"].str.contains("Braintree")) & (df_transaction["Payment Schedule"].notna()) \
           & (df_transaction["msnfp_constituentnumber"].isnull()) & (df_transaction["match_prob"].isnull()),
           (df_transaction["Data Entry Reference"].str.contains("Braintree") == False) \
           & (df_transaction["msnfp_constituentnumber"].notna()) & (df_transaction["match_prob"] < match_threshold),
           (df_transaction["Data Entry Reference"].str.contains("Braintree") == False) \
           & (df_transaction["msnfp_constituentnumber"].isnull()) & (df_transaction["match_prob"].isnull())],
          ["check_conID", "check_conID", "new_conID_new_payschedID", "check_conID_new_payschedID", "new_payschedID", "new_conID",
           "check_conID", "new_conID"],
          default = "all_good")

# Upload file to Fundraising SharePoint in `Single Giving` folder
buffer = StringIO()
df_transaction.to_csv(buffer, index = False, header = True)
df_transaction_to_upload = buffer.getvalue()
Site(f"{url_sharepoint}/{site_sharepoint}", version = Version.v365, 
                    authcookie = auth_sharepoint) \
    .Folder("Shared Documents/Fundraising Operations/Single Giving") \
    .upload_file(df_transaction_to_upload, "transactions_with_donor_match_probs.csv")
"""
Sweep across the ESGF metagrid for desired ocean variables and test PI
and Historical run alignment and shortlists for cases where both PI and Historical have both
direct carbon export outputs (expc and epc100) or auxiliary vars (thetao, o2 and so).

"""
import sys
import intake_esgf
from intake_UtilFuncs import *
from intake_esgf_mods.catalog import ESGFCatalog
import datetime
import pandas as pd
pd.set_option('display.width', 2000)  # pretty printing to console
pd.set_option('display.max_columns', None)  # pretty printing to console
from pathlib import Path


print_precog_header()

today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")  # get today's flag and hour stamp for saving unique files later on

### Defining paths
download_path = input(
    "Define the download path. Either type path or drag desired download parent folder onto this terminal window:")
download_path = Path(download_path.strip(" "))  # strip added as dragging onto terminal adds a trailing 'space'

if os.path.isdir(download_path) == False:
    os.mkdir(download_path)

filename = os.path.join(os.path.normpath(download_path),
                        f"ESGF_search_{today}" + ".xlsx") #dataframe with master search results
logfilename = os.path.join(os.path.normpath(download_path),
                           f"ESGF_search_{today}")

# place it back onto intake_esgf.conf.set(local_cache=XXX)
# if user wishes to retrieve some of the functionality using cached dir
original_cache_path = '~/.esgf/'

#print(intake_esgf.conf)
intake_esgf.conf.set(all_indices=True,
                     local_cache=download_path,
                     confirm_download=True)

print("Printing config. set up for the upcoming ESGF search ...")
print("###########")
print(intake_esgf.conf)
print("###########")

# example of usa to scrape for expc
# cat = ESGFCatalog().search(
#     project='CMIP6',
#     activity_drs=['CMIP', 'ScenarioMIP'],
#     experiment_id=['piControl', 'historical'],
#     frequency='yr',
#     variable_id='expc',
# )
#
# print(cat)
# len(cat.df['source_id'].unique())  # n models with cexp
# models_carb_exp = cat.df['source_id'].unique().sort()

# WHAT WE ARE INTERESTED IN
print("The search will consider as default:")
print("###########")
DefaultSearchParam = {"project" : ['CMIP6'],
            "activity_drs" : ['CMIP', 'ScenarioMIP'],
            "experiment_id" : ['piControl', 'historical'],
            "frequency" : ['mon'],
            "variable_id" : ['user choice'],
            "grid_label" : ['gn', 'gr']}
for key, value in DefaultSearchParam.items():
    print(f"{key}: {value}")
print("###########")

# user prompt for cell measures
var = input("Please enter a list of strings for variable_id. E.g. ['expc'] or multiple ['expc', 'o2', 'thetao', 'so', 'epc100']:")
var = var.strip(" ")
print(f"User entered variable_ids: {var}")
variable_ids = eval(var)

experiments_must_haves = ['piControl', 'historical'] # must have PI and historical in the search.
variables_of_interest = ['expc', 'o2', 'thetao', 'so', 'epc100'] # default hard-backed list of variables of interest
DicDataframeSearches = {'variable_names': [], 'search_results':[]} #initialising empty dic to facilitate saving data

for oceanvar in variable_ids:
    if oceanvar.lower() in variables_of_interest:
        cat =ESGFCatalog().search(project='CMIP6',
                                    activity_drs=['CMIP', 'ScenarioMIP'],
                                    experiment_id=['piControl', 'historical'],
                                    frequency='mon',
                                    variable_id= oceanvar,
                                    grid_label=['gn', 'gr'])

        print(cat.model_groups().to_string())
        cat = cat.remove_ensembles()  # filters out to keep only a single member from the ensemble (i.e., the one with the lowest variant label (see below)
        print(cat.model_groups().to_string())

        # Complete TODO add function to esgf-intake catalog.py file, so that download is not automatic, ...
        # rather we'd like to retrieve the attributes from the search across all nodes into a DataFrame ...
        # so we can see what is available and where, and do further filtering without having to download heaps of data unnecessarily.
        info = cat.infos_to_dict(
            quiet=False)  # dictionary structure containing all the urls, SHA256 hashes to enable serialised download
        DataFrameSearch = pd.DataFrame.from_dict(info['https'])  # all with monthly piControl and historical salinity and potential temperature

        # Complete TODO split file name so that columns also display the following facets
        # source_id or aka model used
        # variable_id
        # experiment_id
        # frequency
        # variant_label = r<k>i<l>p<m>f<n>,
        # where k = realization_index
        # l = initialization_index
        # m = physics_index
        # n = forcing_index

        DataFrameSearch_mod = append_cols(PandasDataFrame=DataFrameSearch)

        #updating dictionary
        DicDataframeSearches['search_results'].append(DataFrameSearch_mod)
        DicDataframeSearches['variable_names'].append(oceanvar)

    else:
        raise KeyError(f'Variable {var} not found in default variables of interest {variables_of_interest}')

#Saving search results to single spreadsheet with individual dataframes where each tab is an ocean variable dataframe
with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    for idx, oceanvarname in enumerate(DicDataframeSearches['variable_names']):
        result = DicDataframeSearches['search_results'][idx]
        result.to_excel(writer, sheet_name=oceanvarname)

print(f"###### Catalogue Scraping complete and raw search results saved as a Dataframe on ...")
print(f"{filename} \n")

#########################
print('Now checking if we have complete piControl and Historical runs and consistent availability of grids...')
combine=input("Would you like perform a 'union' validation to check for PI and Historical runs for > 1 ocean variable?\n"
              "This is equivalent to searching for CMIP6 models that have (var1 & var2 & varN) for both PI and Historical runs. Type [y/n]:")
DFCombined = pd.DataFrame(columns=DicDataframeSearches['search_results'][0].columns)


if combine.lower().strip(" ") in ['y', 'yes']:
    input_chosen = input(f"Enter which variables from the search {DicDataframeSearches['variable_names']} to combine in the form ['var1', 'var2', 'varN']:")
    input_chosen = eval(input_chosen)

    chosen_vars = input_chosen ## bundle chosen vars
    loglabelstr = ""
    for var in chosen_vars:
        loglabelstr = loglabelstr + var + "_"
    loglabelstr = loglabelstr + 'log'

    # and add function to concatenate dataframes from dic of chosen vars
    for idx, oceanvarname in enumerate(DicDataframeSearches['variable_names']):
        result = DicDataframeSearches['search_results'][idx]
        print(result)
        DFCombined = pd.concat([DFCombined, result], axis=0)

   ## then pass to catalog traverser
    logger = instantiate_logging_file(logfilename + '_' + loglabelstr + '.txt', logger_name=loglabelstr)
    models_to_discard, model_DF_test_grids_concatenated, df_downloadable = catalog_traverser(logger, DFCombined, chosen_vars)

    models_to_keep = df_downloadable['source_id'].unique().tolist()
    logger.info(f"Models with complete PI and Historical runs for {chosen_vars} in at least one consistent grid ('gn' or 'gr'):")
    for model in sorted(models_to_keep):
        logger.info(model)
    logger.info('########=END=#######')

    # now we have a list of models to keep and also have a reduced dataframe containing all pre-downloadable files for the concatenated dataframe ('df_downloadable').
    # But still need to check if the files can actually be retrieved from endpoints
    # Saving Dataframe searches after performing url checks that tell us if the files can actually be retrieved from endpoints
    print(f'Traversing urls to test server responses for combined vars {chosen_vars}')
    df_downloadable_tested = link_traverser(df_downloadable)
    save_searched_tests(df_downloadable_tested=df_downloadable_tested, downloadpath=download_path)


else:
    #then just pass single var to catalogue traverser function #TODO fix this to do individual testing
    print(f"User entered {combine}. Checking individual variables for PI and Historical run consistency and availability of grids...")

    for idx, oceanvarname in enumerate(DicDataframeSearches['variable_names']):
        chosen_vars = oceanvarname
        print(f"###### Testing {oceanvarname} ######")
        loglabelstr = ""
        loglabelstr = oceanvarname + '_' + 'log'

        ## then pass to catalog traverser
        logger = instantiate_logging_file(logfilename + '_' + loglabelstr + '.txt', logger_name=loglabelstr)
        models_to_discard, model_DF_test_grids_concatenated, df_downloadable = catalog_traverser(logger,
                                                                                                 DicDataframeSearches['search_results'][idx],
                                                                                                 chosen_vars)

        models_to_keep = df_downloadable['source_id'].unique().tolist()
        logger.info(
            f"Models with complete piControl and Historical runs for {chosen_vars} in at least one consistent grid ('gn' or 'gr'):")
        for model in sorted(models_to_keep):
            logger.info(model)
        logger.info('########=END=#######')

        # now we have a list of models to keep and also have a reduced dataframe containing all pre-downloadable files ('df_downloadable').
        # But still need to check if the files can actually be retrieved from endpoints
        # Saving Dataframe searches after performing url checks that tell us if the files can actually be retrieved from endpoints
        print(f'Traversing urls to test server responses for var {chosen_vars}')
        df_downloadable_tested = link_traverser(df_downloadable)
        save_searched_tests(df_downloadable_tested=df_downloadable_tested, downloadpath=download_path)

# #### SANITY CHECK #####
# First just checking if filtered full-search result and downloadable are same size when excluding odd-cases for when grid availability is missing:
# dummyDF1 = DataFrameSearch_T_S_o2[DataFrameSearch_T_S_o2['source_id'].isin(models_to_keep_T_S_o2)]
# dummyDF1 = dummyDF1.drop(['HTTPServer', 'OPENDAP', 'Globus'], axis=1)
# dummyDF2 = df_downloadable_T_S_o2.drop(['HTTPServer', 'OPENDAP', 'Globus'], axis=1)
# compareDF = dummyDF1.merge(dummyDF2, how='outer',
#                            indicator=True)  # sanity check (show that odd ones are 'left only' entries) - these are the cases where a variable for one of the grid_labels is missing

#############
# Saving Dataframe searches after performing url checks that tell us if the files can actually be retrieved from endpoints
# print(f'Traversing urls to test for server responses for vars in Shortlisted Dataframe df_downloadable_T_S_o2')
# df_downloadable_T_S_o2 = link_traverser(df_downloadable_T_S_o2)
# save_searched_tests(df_downloadable_tested=df_downloadable_T_S_o2, downloadpath=download_path)
#
# print(f'Traversing urls to test for server responses for vars in Shortlisted Dataframe df_downloadable_expc')
# df_downloadable_expc = link_traverser(df_downloadable_expc)
# save_searched_tests(df_downloadable_tested=df_downloadable_expc, downloadpath=download_path)
#
# print(f'Traversing urls to test for server responses for vars in Shortlisted Dataframe df_downloadable_epc100')
# df_downloadable_epc100 = link_traverser(df_downloadable_epc100)
# save_searched_tests(df_downloadable_tested=df_downloadable_epc100, downloadpath=download_path)
############
# Motivational quote
print_precog_quote()
print(f'Data sweep complete. Dataframes should have been saved at {download_path}')
print(f'Now giddy up and run "~/intake_OceanVarsDL.py" to download the data.')

########### NEXT STEPS ####################
# compelte TODO add logger to print outputs
# complete TODO check if selected ESMs have working downloadable links across all files (row wise check of all indices so that request is different from 404! and requests file header actually points to a file with size > 0)
# complete TODO - New program: download each file in parallel and run check sum after completion to check for file corruption
# TODO - New program : find models after screening for pi and Historical which have forward branching simulations available in for example 'ScenarioMIP' activities
###############################

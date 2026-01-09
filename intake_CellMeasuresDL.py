"""
Sweep across the ESGF metagrid for desired ocean cell variables from
shortlisted models in the Catalogue Dataframes

"""

from multiprocessing.pool import ThreadPool
import numpy as np
import intake_esgf
import pandas as pd
import tqdm
from ascii_magic import AsciiArt
from intake_UtilFuncs import *
from intake_esgf_mods.catalog import ESGFCatalog
pd.set_option('display.width', 2000)  # pretty printing to console
pd.set_option('display.max_columns', None)  # pretty printing to console
import datetime
from intake_OceanVarsDL import download_files
from pathlib import Path


if __name__ == "__main__":

    my_art = AsciiArt.from_image('./misc_images/precog_logo_full.png')
    my_art.to_terminal(width_ratio=3)

    today = datetime.datetime.now().strftime(
        "%Y%m%d-%H%M%S")  # get today's flag and hour stamp for saving unique files later on

    # TODO Prompt user for download path using Tkinter or dragging folder onto terminal

    download_path = '~/Desktop/test_download_CMIP6/'

    if os.path.isdir(download_path) == False:
        os.mkdir(download_path)

    filename = os.path.join(os.path.normpath(download_path), f"ESGF_search_{today}" + ".xlsx")
    logfilename = os.path.join(os.path.normpath(download_path), f"ESGF_search_{today}")

    #####

    original_cache_path = '~/.esgf/'  # place it back if user wishes to retrieve some of the functionality using cached dir
    # print(intake_esgf.conf)
    intake_esgf.conf.set(all_indices=True, local_cache=download_path, confirm_download=True)
    # print(intake_esgf.conf)

    var = input("Please enter the variable name for ocean grid measures [e.g. 'areacello' or 'volcello]':")
    var = var.split(' ')[0]
    print(f"User entered cell variable name: {var}")

    # start the logger
    logger1 = instantiate_logging_file(logfilename + '_' + var + '_log.txt', logger_name=str(var))
    logger1.info("Please enter the variable name for ocean grid measures [e.g. 'areacello' or 'volcello]':")
    logger1.info(f"User entered cell variable name: {var}")
    #####

    ########################
    # scrape to see which ones to include
    cat = ESGFCatalog().search(
        project='CMIP6',
        # activity_drs=['CMIP', 'ScenarioMIP'],
        # experiment_id=['piControl', 'historical'],
        variable_id=var,
    )

    print(cat)
    len(cat.df['source_id'].unique())  # n models with cell var
    logger1.info(f'There are {len(cat.df['source_id'].unique())} models with var {var} available.')
    models_cellmeasure = cat.df['source_id'].unique().sort()
    logger1.info(models_cellmeasure)

    # dictionary structure containing all the urls, SHA256 hashes to enable serialised download
    info = cat.infos_to_dict(quiet=False)
    DataFrameSearch = pd.DataFrame.from_dict(info['https'])

    col_names = ['source_id', 'experiment_id', 'variant_label', 'frequency', 'variable_id', 'grid_label']
    positional_order = [3, 4, 5, 6, 7, 8]

    logger1.info(f'Building dataframe with catalogue search results for var {var}...')
    DF_cellmeasure = append_cols(PandasDataFrame=DataFrameSearch,
                                 new_col_names=col_names,
                                 positional_order=positional_order)

    # Complete TODO import all DF filtered searches and then concat source_ids to have a single list of target models.

    logger1.info(f"Importing Dataframes from filtered searches for ocean variables...")
    DF_master = pd.DataFrame()
    # import DFs
    for file in os.listdir(download_path):
        if 'DF_Downloadable' in file and file.endswith(".xlsx") and not ('areacello' in file or 'volcello' in file):
            df_filename = os.path.join(download_path, file)
            DF_dummy = pd.read_excel(df_filename)
            DF_master = pd.concat([DF_master, DF_dummy])

    # Complete TODO find corresponding models based on filtered search
    var_id = DF_master['variable_id'].unique()
    logger1.info(f"Dataframe for variables {var_id} imported.")

    models_target = DF_master['source_id'].unique().tolist()
    test_list = [model in DF_cellmeasure['source_id'].unique().tolist() for model in models_target]

    # Check if all models shortlisted have been picked up in the search for cell measure (either areacello or volcello, depends on what the user entered):

    logger1.info(f'Checking all shortlisted models to find corresponding {var} file:')
    print(f'Checking all shortlisted models to find corresponding {var} file:')

    for test, model in zip(test_list, models_target):
        print(f"Found {var} for model {model}? {test}")
        logger1.info(f"Found {var} for model {model}? {test}")

    print('Summary of test was:')
    logger1.info('Summary of test was:')

    if all(test_list):
        print(f"Found cell variable {var} for all models shortlisted for var {var_id}:")
        logger1.info(f"Found cell variable {var} for all models shortlisted for var {var_id}:")

    else:
        res = [i for i, val in enumerate(test_list) if not val]
        for idx in res:
            print(f"Cell variable {var} for model {models_target[idx]} not found.")
            logger1.info(f"Cell variable {var} for model {models_target[idx]} not found.")

    # Now traverse to fnd best match for cell measure
    # First find target unique keys
    indices_to_grab = []
    keys = DF_master[
        'key'].unique().tolist()  # these need to be stripped of var_ids and frequency and replaced with right string
    for key in keys:
        parts = key.split('.')
        for idx in range(0, len(parts)):
            if parts[idx] == 'Omon':
                parts[idx] = 'Ofx'

            if parts[
                idx] in var_id:  # if the part of the key has any of the vars (theta, o2, so, epc100) then replace by 'areacello' or another cell measure
                parts[idx] = var
        key = '.'.join(parts)
        # print(key)

        idx = DF_cellmeasure.index[DF_cellmeasure['key'] == key]
        if idx.size != 0:
            idx = int(idx[0])
            indices_to_grab.append(idx)

    indices_to_grab = np.unique(indices_to_grab)

    DF_cellmeasure_filtered = DF_cellmeasure[DF_cellmeasure.index.isin(indices_to_grab)]

    ####### now relaxing criteria and creating regex to get cell measure from another variant label (e.g., *r2i1p1f2) and concat to DF_cellmeasure filtered

    keys = DF_master['key']  # these need to be stripped of var_ids and frequency and replaced with right string
    keys_target_wild = []
    for key in keys:
        parts = key.split('.')
        parts[
            0:3] = '*'  # with each replacement, the list shrinks so indexes change. That's why we need to keep clumping and breaking
        key = '.'.join(parts)
        parts = key.split('.')
        parts[2:5] = '*'
        key = '.'.join(parts)
        parts = key.split('.')
        for idx in range(0, len(parts)):
            if parts[
                idx] in var_id:  # if the part of the key has any of the vars (theta, o2, so, epc100) then replace by 'areacello' or another cell measure
                parts[idx] = var

        key = '.'.join(parts)  # this key should then get a wildcard '*' added
        keys_target_wild.append(key)

    DF_master['key_wildcards'] = keys_target_wild

    # now onto the regex step
    keys_cm = DF_cellmeasure['key']
    keys_cm_wild = []
    for key in keys_cm:
        parts = key.split('.')
        parts[
            0:3] = '*'  # with each replacement, the list shrinks so indexes change. That's why we need to keep clumping and breaking
        key = '.'.join(parts)
        parts = key.split('.')
        parts[2:5] = '*'
        key = '.'.join(parts)  # this key should then get a wildcard '*' added
        keys_cm_wild.append(key)

    DF_cellmeasure['key_wildcards'] = keys_cm_wild

    model_found = []
    DF_cellmeasure_relaxed_regex = pd.DataFrame(columns=DF_cellmeasure.columns)

    DF_cellmeasure_relaxed_regex[
        "file_start"] = None  # this is added to suppress verbose warnings for columns where date-time types in 'file_start' and 'file_end' cols show inconsistencies in data types
    DF_cellmeasure_relaxed_regex["file_end"] = None


    ##################
    # now looping through all regex matches for each model (sometimes we get over 100s of hits per single model for areacello or volcello)
    ##################
    for row in tqdm(range(0, len(DF_master)), total=len(DF_master)):
        # print(row)
        DF_single = DF_master.iloc[row]
        target_key = DF_single['key_wildcards']
        model = DF_single['source_id']

        if model not in model_found:  # will skip loop if next row in master dataframe represents a model that's been searched already

            matches = DF_cellmeasure.index[DF_cellmeasure[
                                               'key_wildcards'] == target_key]  # these are indices where a match in the 'wild_cards' from both dataframes were found
            if matches.size != 0:
                print(f"Found possible matches for {target_key} in cell measure dataframe for model {model}.")
                print(f"Indices for files in cell measure dataframe are {matches}")
                model_found.append(model)

                ##now check if all shortlisted file sizes are the same
                DF_cellmeasure_shortlisted = DF_cellmeasure[DF_cellmeasure.index.isin(matches)]
                DF_cellmeasure_selected = DF_cellmeasure_shortlisted.iloc[
                    [0]]  # copy first line to initiate dataframe that will be concatenated

                # account for cases where file size values are different and shrink dataframe to shortlist one instance of each option and download both to check later
                vals = DF_cellmeasure_shortlisted['size'].unique().tolist()
                processed = []
                for val in vals:
                    if val not in processed:
                        for idx in range(0, len(DF_cellmeasure_shortlisted)):
                            if DF_cellmeasure_shortlisted.iloc[idx]['size'] == val:
                                DF_cellmeasure_selected = pd.concat(
                                    [DF_cellmeasure_selected, DF_cellmeasure_shortlisted.iloc[[idx]]])
                                DF_cellmeasure_selected = DF_cellmeasure_selected.iloc[1:]
                                # print(f"Processed {val}")
                                processed.append(val)
                                # print(len(processed))
                                break  # break from checking size loop as only a single copy is needed to be appended

                ##now appending results back to filtered dataframe
                DF_cellmeasure_relaxed_regex = pd.concat([DF_cellmeasure_relaxed_regex, DF_cellmeasure_selected])

    ######################
    len(DF_cellmeasure_relaxed_regex['size'].unique().tolist())

    # this is where I stop - resume
    # todo subset per model and then save a dataframe for the cell measure inside the model folder to check in the future
    # todo keep only two versions of cell measure in case there are duplicate files found. One for the larger file and one for the smallest file and pass these to be added to final dataframe

    for model in [model_found[1]]:
        df_sub = DF_cellmeasure_relaxed_regex[DF_cellmeasure_relaxed_regex['source_id'] == model]
        df_sub = df_sub.iloc[1:]  # removes first line which invariably contains a copy

    # build a patch with a column containing a simpler local path so that files are saved closer to model root directory rather than down a tree of directories:
    paths = []
    for row in range(0, len(DF_cellmeasure_filtered)):
        model = DF_cellmeasure_filtered.iloc[row]['source_id']
        file_name = DF_cellmeasure_filtered.iloc[row]['path']
        file_name = file_name.name
        path = os.path.join(download_path, 'CMIP6', model, var,
                            file_name)  # this is the directory structure where cell measures are saved
        paths.append(path)

    ##################
    # now check if urls are downloadable
    ##################
    print('Checking server responses from file urls...')
    logger1.info('Checking server responses from file urls...')

    df_downloadable = link_traverser(DF_cellmeasure_filtered)

    print('Appending simpler local paths for saving individual files...')
    logger1.info('Appending simpler local paths for saving individual files...')

    df_downloadable['local_path'] = paths
    save_searched_tests(df_downloadable_tested=df_downloadable, downloadpath=download_path)
    print(f"Checks complete. Dataframe exported to {download_path}")
    logger1.info(f"Checks complete. Dataframe exported to {download_path}")
    logger1.info('########=END=#######\n')

    ##################
    # now onto importing the dataframe containing filtered results and doing downloads
    ##################

    df_filename = input("Now either drag onto terminal or type path to Dataframe with the Filtered ESGF search results:")
    df_filename = Path(df_filename.strip(" ")).name  # strip added as dragging onto terminal adds a trailing 'space'

    print('Checking if Dataframe is readable')
    print(os.path.isfile(os.path.join(download_path, df_filename)))
    print(f'Importing Dataframe {df_filename}')

    df_downloadable = pd.read_excel(os.path.join(download_path, df_filename))

    ## NOW ONTO DOWNLOADING FILES
    # Complete TODO prompt for user input
    print(
        f'There are {len(df_downloadable)} files totalling {(sum(df_downloadable['size']) / 1e9):.2f} Gb for variable(s) {df_downloadable['variable_id'].unique().tolist()}')
    user_input = input("Do you want to continue to downloads? (yes/no): ")
    user_input = user_input.strip(" ")
    if user_input.lower() in ["yes", "y"]:
        print("Continuing...\n")

        # parallelizing download
        iterable_dwnld = []
        for i in range(0, len(df_downloadable)):
            # building_iterable
            df_single = df_downloadable.iloc[[i]]
            iterable_dwnld.append(
                (df_single, download_path))  # this is the iterable being passed to the download function with 2 args

        #test_iterable = iterable_dwnld[0:4]  # this is for testing. #TODO delete this line in the future

        with  ThreadPool(min(32, os.cpu_count() + 4)) as pool:
            pool.starmap(download_files, iterable_dwnld, chunksize=4)  # starmap unpacks the iterable args to function

        # TODO check if all files were downloaded to destination and append file status to dataframe

        print('Downloads complete \n')

    else:
        print("Exiting...\n")

    # Motivational quote
    end_art = AsciiArt.from_image('./misc_images/squid.png')
    end_art.to_terminal(columns=100)
    print('You got the data. Now go be amazing!')






















































